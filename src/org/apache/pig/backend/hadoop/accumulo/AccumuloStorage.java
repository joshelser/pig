/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.pig.backend.hadoop.accumulo;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.log4j.Logger;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.PeekingIterator;

/**
 * Basic PigStorage implementation that uses Accumulo as the backing store.
 * 
 * <p>
 * When writing data, the first entry in the {@link Tuple} is treated as the 
 * row in the Accumulo key, while subsequent entries in the tuple are handled
 * as columns in that row. {@link Map}s are expanded, placing the map key in 
 * the column family and the map value in the Accumulo value. Scalars are placed
 * directly into the value with an empty column qualifier. If the columns argument
 * on the constructor is omitted, null or the empty String, no column family is provided
 * on the Keys created for Accumulo
 * </p>
 * 
 * <p>
 * When reading data, if aggregateColfams is true, elements in the same row and column
 * family are aggregated into a single {@link Map}. This will result in a {@link Tuple} of
 * length (unique_column_families + 1) for the given row. If aggregateColfams is false, column 
 * family and column qualifier are concatenated (separated by a colon), and placed into a 
 * {@link Map}. This will result in a {@link Tuple} with two entries, where the latter element
 * has a number of elements equal to the number of columns in the given row.
 * </p>
 */
public class AccumuloStorage extends AbstractAccumuloStorage {
  private static final Logger log = Logger.getLogger(AccumuloStorage.class);
  private static final String COLON = ":", EMPTY = "";
  
  public static final String METADATA_SUFFIX = "_metadata";
  
  // Not sure if AccumuloStorage instances need to be thread-safe or not
  final Text _cfHolder = new Text(), _cqHolder = new Text();
  
  /**
   * Contains column families prefixes (without asterisk) that were fetched
   */
  protected TreeSet<Text> colfamPrefixes = new TreeSet<Text>();
  
  /**
   * Contains column families to column qualifier prefixes (without asterisk) that were fetched
   */
  protected TreeMap<Text,Text> colqualPrefixes = new TreeMap<Text,Text>();
  
  /**
   * Creates an AccumuloStorage which writes all values in a {@link Tuple} with an empty column family
   * and doesn't group column families together on read (creates on {@link Map} for all columns)
   */
  public AccumuloStorage() throws ParseException, IOException {
    this(EMPTY, EMPTY);
  }
  
  /**
   * Create an AccumuloStorage with a CSV of columns-families to use on write and whether columns in a row
   * should be grouped by family on read. 
   * @param columns
   *          A comma-separated list of column families to use when writing data, aligned to the n'th entry in the tuple
   * @param aggregateColfams 
   *          Should unique column qualifier and value pairs be grouped together by column family when reading data
   */
  public AccumuloStorage(String columns) throws ParseException, IOException {
    this(columns, EMPTY);
  }
  
  public AccumuloStorage(String columnStr, String args) throws ParseException, IOException {
    super(columnStr, args);
    
    
    for (Column col : this.columns) {
      switch(col.getType()) {
        case COLFAM_PREFIX:
          int colfIndex = col.getColumnFamily().indexOf(ASTERISK);
          
          // Empty colfam should just match everything
          if (-1 == colfIndex) {
            throw new IOException("Did not find asterisk in: " + col.getColumnFamily());
          } else {
            colfamPrefixes.add(new Text(col.getColumnFamily().substring(0, colfIndex)));
          }
          
          break;
        case COLQUAL_PREFIX:
          int colqIndex = col.getColumnQualifier().indexOf(ASTERISK);
          
          if (-1 == colqIndex) {
            throw new IOException("Did not find asterisk in: "+ col.getColumnQualifier());
          } else {
            colqualPrefixes.put(new Text(col.getColumnFamily()), new Text(col.getColumnQualifier().substring(0, colqIndex)));
          }
          break;
        default:
          break;
      }
    }
  }
  
  @Override
  protected Tuple getTuple(Key key, Value value) throws IOException {
    SortedMap<Key,Value> rowKVs = WholeRowIterator.decodeRow(key, value);
    
    List<Object> tupleEntries = Lists.newLinkedList();
    PeekingIterator<Entry<Key,Value>> iter = Iterators.peekingIterator(rowKVs.entrySet().iterator());
    Entry<Key,Value> currentEntry;
    Map<String,DataByteArray> aggregateMap;
    final Text cfHolder = new Text();
    final Text cqHolder = new Text();
    
    while (iter.hasNext()) {
      currentEntry = iter.next();
      Key currentKey = currentEntry.getKey();
      Value currentValue = currentEntry.getValue();
      currentKey.getColumnFamily(cfHolder);
      currentKey.getColumnQualifier(cqHolder);
      
      // Colfam prefixes take priority over colqual prefixes
      if (colfamPrefixes.contains(cfHolder)) {
        aggregateMap = new HashMap<String,DataByteArray>();
        
        aggregateMap.put(cfHolder + COLON + cqHolder, 
            new DataByteArray(currentValue.get()));
        
        // Aggregate as long as we match the given prefix
        Text colfamToAggregate = new Text(cfHolder);
        while (iter.hasNext()) {
          currentEntry = iter.peek();
          currentKey = currentEntry.getKey();
          currentValue = currentEntry.getValue();
          currentKey.getColumnFamily(cfHolder);
          currentKey.getColumnQualifier(cqHolder);
          
          if (prefixMatch(cfHolder, colfamToAggregate)) {
            // Consume the key/value
            iter.next();
            aggregateMap.put(cfHolder + COLON + cqHolder, 
                new DataByteArray(currentValue.get()));
          }
        }
        
        tupleEntries.add(aggregateMap);
      } else if (colqualPrefixes.containsKey(cfHolder)) {
        aggregateMap = new HashMap<String,DataByteArray>();
        
        // We're aggregating some portion of the colqual in this colfam
        Text desiredColqualPrefix = colqualPrefixes.get(cfHolder);
        
        if (prefixMatch(cqHolder, desiredColqualPrefix)) {
          aggregateMap = new HashMap<String,DataByteArray>();
          aggregateMap.put(cfHolder + COLON + cqHolder, new DataByteArray(currentValue.get()));
          
          Text colfamToAggregate = new Text(cfHolder);
          while (iter.hasNext()) {
            currentEntry = iter.peek();
            currentKey = currentEntry.getKey();
            currentValue = currentEntry.getValue();
            currentKey.getColumnFamily(cfHolder);
            currentKey.getColumnQualifier(cqHolder);
            
            if (colfamToAggregate.equals(cfHolder) && prefixMatch(cqHolder, desiredColqualPrefix)) {
              iter.next();
              aggregateMap.put(cfHolder + COLON + cqHolder, new DataByteArray(currentValue.get()));
            }
          }
          
          tupleEntries.add(aggregateMap);
        } else {
          // When we don't, we had to match this colqual because it was specifically chosen
          tupleEntries.add(new DataByteArray(currentValue.get()));
        }
      } else {
        // It's a literal, just add it
        tupleEntries.add(new DataByteArray(currentValue.get()));
      }
    }
    
    // and wrap it in a tuple
    Tuple tuple = TupleFactory.getInstance().newTuple(tupleEntries.size() + 1);
    tuple.set(0, key.getRow().toString());
    int i = 1;
    for (Object obj : tupleEntries) {
      tuple.set(i, obj);
      i++;
    }
    
    return tuple;
  }
  
  /**
   * Returns true if the value starts with prefix
   * @param value
   * @param prefix
   * @return
   */
  protected boolean prefixMatch(Text value, Text prefix) {
    // Can't match a longer prefix
    if (value.getLength() < prefix.getLength()) {
      return false;
    }
    
    ByteBuffer valueBuf = ByteBuffer.wrap(value.getBytes(), 0, value.getLength()),
        prefixBuf = ByteBuffer.wrap(prefix.getBytes(), 0, prefix.getLength());
    
    for (int i = 0; i < prefixBuf.limit(); i++) {
      if (valueBuf.get(i) != prefixBuf.get(i)) {
        return false;
      }
    }
    
    return true;
  }
  
  protected Map<String,Object> aggregate(List<Entry<Key,Value>> columns) {
    final Map<String,Object> map = new HashMap<String,Object>();
    final StringBuilder sb = new StringBuilder(128);
    
    for (Entry<Key,Value> column : columns) {
      String cf = column.getKey().getColumnFamily().toString(), cq = column.getKey().getColumnQualifier().toString();
      
      sb.append(cf);
      if (!cq.isEmpty()) {
        sb.append(COLON).append(cq);
      }
      
      map.put(sb.toString(), new DataByteArray(column.getValue().get()));
      
      sb.setLength(0);
    }
    
    return map;
  }
  
  @Override
  protected void configureInputFormat(Job job) {
    AccumuloInputFormat.addIterator(job, new IteratorSetting(100, WholeRowIterator.class));
  }
  
  @Override
  public Collection<Mutation> getMutations(Tuple tuple) throws ExecException, IOException {
    final ResourceFieldSchema[] fieldSchemas = (schema == null) ? null : schema.getFields();
      
    Iterator<Object> tupleIter = tuple.iterator();
    
    if (1 >= tuple.size()) {
      log.debug("Ignoring tuple of size " + tuple.size());
      return Collections.emptyList();
    }
    
    Mutation mutation = new Mutation(objectToText(tupleIter.next(), (null == fieldSchemas) ? null : fieldSchemas[0]));    
    
    int tupleOffset = 1;
    Iterator<Column> columnIter = columns.iterator();
    while (tupleIter.hasNext() && columnIter.hasNext()) {
      Object o = tupleIter.next();
      Column column = columnIter.next();
      
      // Grab the type for this field
      final byte type = schemaToType(o, (null == fieldSchemas) ? null : fieldSchemas[tupleOffset]);

      switch (column.getType()) {
        case LITERAL:
          byte[] bytes = objToBytes(o, type);
          
          if (null != bytes) {
            Value value = new Value(bytes);
            
            // We don't have any column name from non-Maps
            addColumn(mutation, column.getColumnFamily(), column.getColumnQualifier(), value);
          }
          break;
        case COLFAM_PREFIX:
        case COLQUAL_PREFIX:
          @SuppressWarnings("unchecked")
          Map<String,Object> map = (Map<String,Object>) o;
          
          for (Entry<String,Object> entry : map.entrySet()) {
            String key = entry.getKey();
            Object objValue = entry.getValue();
            
            byte valueType = DataType.findType(objValue);
            byte[] mapValue = objToBytes(objValue, valueType);
            
            if (Column.Type.COLFAM_PREFIX == column.getType()) {
              addColumn(mutation, column.getColumnFamily() + key, null, new Value(mapValue));
            } else if (Column.Type.COLQUAL_PREFIX == column.getType()) {
              addColumn(mutation, column.getColumnFamily(), column.getColumnQualifier() + key, new Value(mapValue));
            } else {
              throw new IOException("Unknown column type");
            }
          }
          break;
        default:
          log.info("Ignoring unhandled column type");
          continue;
      }
      
      tupleOffset++;
    }
    
    if (0 == mutation.size()) {
      return Collections.emptyList();
    }
    
    return Collections.singletonList(mutation);
  }
  
  /**
   * Adds the given column family, column qualifier and value to the given mutation
   * 
   * @param mutation
   * @param colfam
   * @param colqual
   * @param columnValue
   */
  protected void addColumn(Mutation mutation, String colfam, String colqual, Value columnValue) {
    if (null != colfam) {
      _cfHolder.set(colfam);
    } else {
      _cfHolder.clear();
    }
    
    if (null != colqual) {
      _cqHolder.set(colqual);
    } else {
      _cqHolder.clear();
    }
    
    mutation.put(_cfHolder, _cqHolder, columnValue);
  }
}

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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.log4j.Logger;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.builtin.Utf8StorageConverter;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import com.google.common.collect.Lists;

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
  private static final String COMMA = ",", COLON = ":", EMPTY = "";
  
  public static final String METADATA_SUFFIX = "_metadata";
  
  protected final List<String> columnSpecs;
  protected final boolean aggregateColfams;
  
  // Not sure if AccumuloStorage instances need to be thread-safe or not
  final Text _cfHolder = new Text(), _cqHolder = new Text();
  
  /**
   * Creates an AccumuloStorage which writes all values in a {@link Tuple} with an empty column family
   * and doesn't group column families together on read (creates on {@link Map} for all columns)
   */
  public AccumuloStorage() {
    this(EMPTY);
  }
  
  /**
   * Creates an AccumuloStorage with CSV of column families to use on write that doesn't group 
   * column families together on read (creates one {@link Map} for all columns) 
   * @param columns
   *          A comma-separated list of column families to use when writing data, aligned to the n'th entry in the Tuple
   */
  public AccumuloStorage(String columns) {
    this(columns, false);
  }
  
  /**
   * Creates an AccumuloStorage which writes all values in a {@link Tuple} with an empty column family
   * and defers to the provided argument as to whether or not column families are grouped together
   * on read.
   * @param aggregateColfams
   *          Should unique column qualifier and value pairs be grouped together by column family when reading data
   */
  public AccumuloStorage(boolean aggregateColfams) {
    this(EMPTY, aggregateColfams);
  }
  
  /**
   * Create an AccumuloStorage with a CSV of columns families to use on write and whether columns in a row
   * should be grouped by family on read. 
   * @param columns
   *          A comma-separated list of column families to use when writing data, aligned to the n'th entry in the tuple
   * @param aggregateColfams 
   *          Should unique column qualifier and value pairs be grouped together by column family when reading data
   */
  public AccumuloStorage(String columns, String aggregateColfams) {
    this(columns, Boolean.parseBoolean(aggregateColfams));
  }
  
  /**
   * Create an AccumuloStorage with a CSV of columns-families to use on write and whether columns in a row
   * should be grouped by family on read. 
   * @param columns
   *          A comma-separated list of column families to use when writing data, aligned to the n'th entry in the tuple
   * @param aggregateColfams 
   *          Should unique column qualifier and value pairs be grouped together by column family when reading data
   */
  public AccumuloStorage(String columns, boolean aggregateColfams) {
    this.caster = new Utf8StorageConverter();
    this.aggregateColfams = aggregateColfams;
    
    // TODO It would be nice to have some other means than enumerating
    // the CF for every column in the Tuples we're going process
    if (!StringUtils.isBlank(columns)) {
      String[] columnArray = StringUtils.split(columns, COMMA);
      columnSpecs = Lists.newArrayList(columnArray);
    } else {
      columnSpecs = Collections.emptyList();
    }
  }
  
  @Override
  protected Tuple getTuple(Key key, Value value) throws IOException {
    SortedMap<Key,Value> rowKVs = WholeRowIterator.decodeRow(key, value);
    
    List<Object> tupleEntries = Lists.newLinkedList();
    Iterator<Entry<Key,Value>> iter = rowKVs.entrySet().iterator();
    List<Entry<Key,Value>> aggregate = Lists.newLinkedList();
    Entry<Key,Value> currentEntry = null;
    
    while (iter.hasNext()) {
      if (null == currentEntry) {
        currentEntry = iter.next();
        aggregate.add(currentEntry);
      } else {
        Entry<Key,Value> nextEntry = iter.next();
        
        // If we're not aggregating colfams together, or we are and we have the same colfam
        if (!aggregateColfams || currentEntry.getKey().equals(nextEntry.getKey(), PartialKey.ROW_COLFAM)) {
          // Aggregate this entry into the map
          aggregate.add(nextEntry);
        } else {
          currentEntry = nextEntry;
          
          // Flush and start again
          Map<String,Object> map = aggregate(aggregate);
          tupleEntries.add(map);
          
          aggregate = Lists.newLinkedList();
          aggregate.add(currentEntry);
        }
      }
    }
    
    if (!aggregate.isEmpty()) {
      tupleEntries.add(aggregate(aggregate));
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
    AccumuloInputFormat.addIterator(job, new IteratorSetting(50, WholeRowIterator.class));
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
    
    int columnOffset = 0;
    int tupleOffset = 1;
    while (tupleIter.hasNext()) {
      Object o = tupleIter.next();
      String family = null;
      
      // Figure out if the user provided a specific columnfamily to use.
      if (columnOffset < columnSpecs.size()) {
        family = columnSpecs.get(columnOffset);
      }
      
      // Grab the type for this field
      final byte type = schemaToType(o, (null == fieldSchemas) ? null : fieldSchemas[tupleOffset]);
      
      // If we have a Map, we want to treat every Entry as a column in this record
      // placing said column in the column family unless this instance of AccumuloStorage
      // was provided a specific columnFamily to use, in which case the entry's column is
      // in the column qualifier.
      if (DataType.MAP == type) {
        @SuppressWarnings("unchecked")
        Map<String,Object> map = (Map<String,Object>) o;
        
        for (Entry<String,Object> entry : map.entrySet()) {
          Object entryObject = entry.getValue();
          
          // Treat a null value in the map as the lack of this column
          // The input may have come from a structured source where the
          // column could not have been omitted. We can handle the lack of the column
          if (null != entryObject) {
            byte entryType = DataType.findType(entryObject);
            Value value = new Value(objToBytes(entryObject, entryType));
            
            addColumn(mutation, family, entry.getKey(), value);
          }
        }
      } else {
        byte[] bytes = objToBytes(o, type);
        
        if (null != bytes) {
          Value value = new Value(bytes);
          
          // We don't have any column name from non-Maps
          addColumn(mutation, family, null, value);
        }
      }
      
      columnOffset++;
      tupleOffset++;
    }
    
    if (0 == mutation.size()) {
      return Collections.emptyList();
    }
    
    return Collections.singletonList(mutation);
  }
  
  /**
   * Adds column and value to the given mutation. A columnfamily and optional column qualifier
   * or column qualifier prefix is pulled from {@link columnDef} with the family and qualifier 
   * delimiter being a colon. If {@link columnName} is non-null, it will be appended to the qualifier.
   * 
   * If both the {@link columnDef} and {@link columnName} are null, nothing is added to the mutation
   * 
   * @param mutation
   * @param columnDef
   * @param columnName
   * @param columnValue
   */
  protected void addColumn(Mutation mutation, String columnDef, String columnName, Value columnValue) {
    if (null == columnDef && null == columnName) {
      // TODO Emit a counter here somehow? org.apache.pig.tools.pigstats.PigStatusReporter
      log.warn("Was provided no name or definition for column. Ignoring value");
      return;
    }
    
    if (null != columnDef) {
      // use the provided columnDef to make a cf (with optional cq prefix)
      int index = columnDef.indexOf(COLON);
      if (-1 == index) {
        _cfHolder.set(columnDef);
        _cqHolder.clear();
        
      } else {
        byte[] cfBytes = columnDef.getBytes();
        _cfHolder.set(cfBytes, 0, index);
        _cqHolder.set(cfBytes, index + 1, cfBytes.length - (index + 1)); 
      }
    } else {
      _cfHolder.clear();
      _cqHolder.clear();
    }
    
    // If we have a column name (this came from a Map)
    // append that name on the cq.
    if (null != columnName) {
      byte[] cnBytes = columnName.getBytes();
      
      // CQ is either empty or has a prefix from the columnDef
      _cqHolder.append(cnBytes, 0, cnBytes.length);
    }
    
    mutation.put(_cfHolder, _cqHolder, columnValue);
  }
}

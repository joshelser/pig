/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.pig.backend.hadoop.accumulo;

import org.apache.commons.lang.StringUtils;

import com.google.common.base.Preconditions;

/**
 * 
 */
public class Column {

  public static enum Type {
    LITERAL,
    COLFAM_PREFIX,
    COLQUAL_PREFIX,
  }
  
  private final Type columnType;
  private final String columnFamily;
  private final String columnQualifier;
  
  public Column(String col) {
    Preconditions.checkNotNull(col);
    
    String strippedCol = StringUtils.strip(col);
    
    int index = strippedCol.indexOf(AbstractAccumuloStorage.COLON);
    if (-1 == index) {
      columnFamily = strippedCol;
      columnQualifier = null;
      
      if (columnFamily.endsWith(AbstractAccumuloStorage.ASTERISK)) {
        columnType = Type.COLFAM_PREFIX;
      } else {
        columnType = Type.LITERAL;
      }
    } else {
      if (1 == strippedCol.length()) {
        throw new IllegalArgumentException("Cannot parse '" + strippedCol + "'");
      }
      
      columnFamily = strippedCol.substring(0, index);
      columnQualifier = strippedCol.substring(index + 1);
      
      // TODO Handle colf*:colq* ?
      if (columnFamily.endsWith(AbstractAccumuloStorage.ASTERISK)) {
        columnType = Type.COLFAM_PREFIX;
      } else if (columnQualifier.isEmpty() || columnQualifier.endsWith(AbstractAccumuloStorage.ASTERISK)) {
        columnType = Type.COLQUAL_PREFIX;
      } else {
        columnType = Type.LITERAL;
      }
    }
  }
  
  public Type getType() {
    return columnType;
  }
  
  public String getColumnFamily() {
    return columnFamily;
  }
  
  public String getColumnQualifier() {
    return columnQualifier;
  }
  
  @Override
  public boolean equals(Object o) {
    if (o instanceof Column) {
      Column other = (Column) o;

      if (null != columnFamily) {
        if (null == other.columnFamily) {
          return false;
        } else if (!columnFamily.equals(other.columnFamily)) {
          return false;
        }
      }
      
      if (null != columnQualifier) {
        if (null == other.columnQualifier) {
          return false;
        } else if (!columnQualifier.equals(other.columnQualifier)) {
          return false;
        }
      }
      
      return columnType == other.columnType;
    }
    
    return false;
  }
  
  @Override
  public String toString() {
    return columnType + " " + columnFamily + ":" + columnQualifier;
  }
}

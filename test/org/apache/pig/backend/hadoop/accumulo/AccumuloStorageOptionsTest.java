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

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.cli.ParseException;
import org.junit.Assert;
import org.junit.Test;

/**
 * 
 */
public class AccumuloStorageOptionsTest {

  @Test
  public void testFetchColumns() throws ParseException, IOException {
    AbstractAccumuloStorage storage = AbstractAccumuloStorageTest.getAbstractAccumuloStorage("cf1,cf2:cq2");

    List<Column> pairs = new LinkedList<Column>();
    pairs.add(new Column("cf1"));
    pairs.add(new Column("cf2:cq2"));

    Assert.assertEquals(pairs, storage.columns);
  }

  @Test
  public void testWriteColumns() throws ParseException, IOException {
    AbstractAccumuloStorage storage = AbstractAccumuloStorageTest.getAbstractAccumuloStorage("foo,bar,baz,foo:bar,foo:baz");

    List<Column> columnNames = Arrays.asList(new Column("foo"), new Column("bar"), new Column("baz"), new Column("foo:bar"), new Column("foo:baz"));

    Assert.assertEquals(columnNames, storage.columns);
  }

  @Test
  public void testAuths() throws ParseException, IOException {
    AbstractAccumuloStorage storage = AbstractAccumuloStorageTest.getAbstractAccumuloStorage("", "--authorizations auth1,auth2");

    Authorizations auths = new Authorizations("auth1,auth2");

    Assert.assertEquals(auths, storage.authorizations);

    storage = AbstractAccumuloStorageTest.getAbstractAccumuloStorage("", "-auths auth1,auth2");
    Assert.assertEquals(auths, storage.authorizations);
  }

  @Test
  public void testStartEndRows() throws ParseException, IOException {
    AbstractAccumuloStorage storage = AbstractAccumuloStorageTest.getAbstractAccumuloStorage("", "--start begin --end finish");

    Assert.assertEquals("begin", storage.start);
    Assert.assertEquals("finish", storage.end);

    storage = AbstractAccumuloStorageTest.getAbstractAccumuloStorage("", "-s begin -e finish");
    Assert.assertEquals("begin", storage.start);
    Assert.assertEquals("finish", storage.end);
  }

  @Test
  public void testBatchWriterOptions() throws ParseException, IOException {
    long buffSize = 1024 * 50;
    int writeThreads = 8, maxLatency = 30 * 1000;

    AbstractAccumuloStorage storage = AbstractAccumuloStorageTest.getAbstractAccumuloStorage("", "--mutation-buffer-size " + buffSize + " --write-threads "
        + writeThreads + " --max-latency " + maxLatency);

    Assert.assertEquals(buffSize, storage.maxMutationBufferSize);
    Assert.assertEquals(writeThreads, storage.maxWriteThreads);
    Assert.assertEquals(maxLatency, storage.maxLatency);
    
    storage = AbstractAccumuloStorageTest.getAbstractAccumuloStorage("", "-buff " + buffSize + " -wt "
        + writeThreads + " -ml " + maxLatency);

    Assert.assertEquals(buffSize, storage.maxMutationBufferSize);
    Assert.assertEquals(writeThreads, storage.maxWriteThreads);
    Assert.assertEquals(maxLatency, storage.maxLatency);
  }

}

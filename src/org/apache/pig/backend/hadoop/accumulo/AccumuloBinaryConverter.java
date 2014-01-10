/* Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Map;

import org.apache.accumulo.core.Constants;
import org.apache.pig.LoadStoreCaster;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.joda.time.DateTime;

/**
 * A LoadStoreCaster implementation which stores most type implementations as bytes generated
 * from the toString representation with a UTF8 Charset. Pulled some implementations from the
 * Accumulo Lexicoder implementations in 1.6.0.
 */
public class AccumuloBinaryConverter implements LoadStoreCaster {

  // TODO There is definitely a better implementation that does not rely on
  // stringification that would be much more efficient.
  
  /**
   * NOT IMPLEMENTED
   */
  @Override
  public DataBag bytesToBag(byte[] b, ResourceFieldSchema fieldSchema) throws IOException {
    throw new ExecException("Can't generate DataBags from byte[]");
  }

  @Override
  public BigDecimal bytesToBigDecimal(byte[] b) throws IOException {
    throw new ExecException("Can't generate a BigInteger from byte[]");
  }

  @Override
  public BigInteger bytesToBigInteger(byte[] b) throws IOException {
    // Taken from Accumulo's BigIntegerLexicoder in 1.6.0
    DataInputStream dis = new DataInputStream(new ByteArrayInputStream(b));
    int len = dis.readInt();
    len = len ^ 0x80000000;
    len = Math.abs(len);

    byte[] bytes = new byte[len];
    dis.readFully(bytes);

    bytes[0] = (byte) (0x80 ^ bytes[0]);

    return new BigInteger(bytes);
  }

  @Override
  public Boolean bytesToBoolean(byte[] b) throws IOException {
    String s = new String(b, Constants.UTF8);
    return Boolean.parseBoolean(s);
  }

  @Override
  public String bytesToCharArray(byte[] b) throws IOException {
    return new String(b, Constants.UTF8);
  }

  /**
   * NOT IMPLEMENTED
   */
  @Override
  public DateTime bytesToDateTime(byte[] b) throws IOException {
    String s = new String(b, Constants.UTF8);
    return DateTime.parse(s);
  }

  @Override
  public Double bytesToDouble(byte[] b) throws IOException {
    String s = new String(b, Constants.UTF8);
    return Double.parseDouble(s);
  }

  @Override
  public Float bytesToFloat(byte[] b) throws IOException {
    String s = new String(b, Constants.UTF8);
    return Float.parseFloat(s);
  }

  @Override
  public Integer bytesToInteger(byte[] b) throws IOException {
    String s = new String(b, Constants.UTF8);
    return Integer.parseInt(s);
  }

  @Override
  public Long bytesToLong(byte[] b) throws IOException {
    String s = new String(b, Constants.UTF8);
    return Long.parseLong(s);
  }

  /**
   * NOT IMPLEMENTED
   */
  @Override
  public Map<String,Object> bytesToMap(byte[] b, ResourceFieldSchema fieldSchema) throws IOException {
    throw new ExecException("Can't generate Map from byte[]");
  }

  /**
   * NOT IMPLEMENTED
   */
  @Override
  public Tuple bytesToTuple(byte[] b, ResourceFieldSchema fieldSchema) throws IOException {
    throw new ExecException("Can't generate a Tuple from byte[]");
  }

  /**
   * Not implemented!
   */
  @Override
  public byte[] toBytes(BigDecimal bd) throws IOException {
    throw new IOException("Can't generate bytes from BigDecimal");
  }

  @Override
  public byte[] toBytes(BigInteger bi) throws IOException {
    // Taken from Accumulo's BigIntegerLexicoder in 1.6.0
    byte[] bytes = bi.toByteArray();

    byte[] ret = new byte[4 + bytes.length];

    DataOutputStream dos = new DataOutputStream(new FixedByteArrayOutputStream(ret));

    // flip the sign bit
    bytes[0] = (byte) (0x80 ^ bytes[0]);

    int len = bytes.length;
    if (bi.signum() < 0)
      len = -len;

    len = len ^ 0x80000000;

    dos.writeInt(len);
    dos.write(bytes);
    dos.close();

    return ret;
  }

  @Override
  public byte[] toBytes(Boolean b) throws IOException {
    return b.toString().getBytes(Constants.UTF8);
  }

  /**
   * NOT IMPLEMENTED
   */
  @Override
  public byte[] toBytes(DataBag bag) throws IOException {
    throw new ExecException("Cant' generate bytes from DataBag");
  }

  @Override
  public byte[] toBytes(DataByteArray a) throws IOException {
    return a.get();
  }

  @Override
  public byte[] toBytes(DateTime dt) throws IOException {
    return dt.toString().getBytes(Constants.UTF8);
  }

  @Override
  public byte[] toBytes(Double d) throws IOException {
    return d.toString().getBytes(Constants.UTF8);
  }

  @Override
  public byte[] toBytes(Float f) throws IOException {
    return f.toString().getBytes(Constants.UTF8);
  }

  @Override
  public byte[] toBytes(Integer i) throws IOException {
    return i.toString().getBytes(Constants.UTF8);
  }

  @Override
  public byte[] toBytes(Long l) throws IOException {
    return l.toString().getBytes(Constants.UTF8);
  }

  /**
   * NOT IMPLEMENTED
   */
  @Override
  public byte[] toBytes(Map<String,Object> m) throws IOException {
    throw new IOException("Can't generate bytes from Map");
  }

  @Override
  public byte[] toBytes(String s) throws IOException {
    return s.getBytes(Constants.UTF8);
  }

  /**
   * NOT IMPLEMENTED
   */
  @Override
  public byte[] toBytes(Tuple t) throws IOException {
    throw new IOException("Can't generate bytes from Tuple");
  }
}

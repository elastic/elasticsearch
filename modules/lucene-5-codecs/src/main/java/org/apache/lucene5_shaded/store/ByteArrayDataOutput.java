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
package org.apache.lucene5_shaded.store;


import org.apache.lucene5_shaded.util.BytesRef;

/**
 * DataOutput backed by a byte array.
 * <b>WARNING:</b> This class omits most low-level checks,
 * so be sure to test heavily with assertions enabled.
 * @lucene.experimental
 */
public class ByteArrayDataOutput extends DataOutput {
  private byte[] bytes;

  private int pos;
  private int limit;

  public ByteArrayDataOutput(byte[] bytes) {
    reset(bytes);
  }

  public ByteArrayDataOutput(byte[] bytes, int offset, int len) {
    reset(bytes, offset, len);
  }

  public ByteArrayDataOutput() {
    reset(BytesRef.EMPTY_BYTES);
  }

  public void reset(byte[] bytes) {
    reset(bytes, 0, bytes.length);
  }
  
  public void reset(byte[] bytes, int offset, int len) {
    this.bytes = bytes;
    pos = offset;
    limit = offset + len;
  }
  
  public int getPosition() {
    return pos;
  }

  @Override
  public void writeByte(byte b) {
    assert pos < limit;
    bytes[pos++] = b;
  }

  @Override
  public void writeBytes(byte[] b, int offset, int length) {
    assert pos + length <= limit;
    System.arraycopy(b, offset, bytes, pos, length);
    pos += length;
  }
}

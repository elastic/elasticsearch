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


import java.io.IOException;
import java.util.Map;
import java.util.Set;

import org.apache.lucene5_shaded.util.BitUtil;
import org.apache.lucene5_shaded.util.BytesRef;

/**
 * Abstract base class for performing write operations of Lucene's low-level
 * data types.
 
 * <p>{@code DataOutput} may only be used from one thread, because it is not
 * thread safe (it keeps internal state like file position).
 */
public abstract class DataOutput {

  /** Writes a single byte.
   * <p>
   * The most primitive data type is an eight-bit byte. Files are 
   * accessed as sequences of bytes. All other data types are defined 
   * as sequences of bytes, so file formats are byte-order independent.
   * 
   * @see IndexInput#readByte()
   */
  public abstract void writeByte(byte b) throws IOException;

  /** Writes an array of bytes.
   * @param b the bytes to write
   * @param length the number of bytes to write
   * @see DataInput#readBytes(byte[],int,int)
   */
  public void writeBytes(byte[] b, int length) throws IOException {
    writeBytes(b, 0, length);
  }

  /** Writes an array of bytes.
   * @param b the bytes to write
   * @param offset the offset in the byte array
   * @param length the number of bytes to write
   * @see DataInput#readBytes(byte[],int,int)
   */
  public abstract void writeBytes(byte[] b, int offset, int length) throws IOException;

  /** Writes an int as four bytes.
   * <p>
   * 32-bit unsigned integer written as four bytes, high-order bytes first.
   * 
   * @see DataInput#readInt()
   */
  public void writeInt(int i) throws IOException {
    writeByte((byte)(i >> 24));
    writeByte((byte)(i >> 16));
    writeByte((byte)(i >>  8));
    writeByte((byte) i);
  }
  
  /** Writes a short as two bytes.
   * @see DataInput#readShort()
   */
  public void writeShort(short i) throws IOException {
    writeByte((byte)(i >>  8));
    writeByte((byte) i);
  }

  /** Writes an int in a variable-length format.  Writes between one and
   * five bytes.  Smaller values take fewer bytes.  Negative numbers are
   * supported, but should be avoided.
   * <p>VByte is a variable-length format for positive integers is defined where the
   * high-order bit of each byte indicates whether more bytes remain to be read. The
   * low-order seven bits are appended as increasingly more significant bits in the
   * resulting integer value. Thus values from zero to 127 may be stored in a single
   * byte, values from 128 to 16,383 may be stored in two bytes, and so on.</p>
   * <p>VByte Encoding Example</p>
   * <table cellspacing="0" cellpadding="2" border="0" summary="variable length encoding examples">
   * <tr valign="top">
   *   <th align="left">Value</th>
   *   <th align="left">Byte 1</th>
   *   <th align="left">Byte 2</th>
   *   <th align="left">Byte 3</th>
   * </tr>
   * <tr valign="bottom">
   *   <td>0</td>
   *   <td><code>00000000</code></td>
   *   <td></td>
   *   <td></td>
   * </tr>
   * <tr valign="bottom">
   *   <td>1</td>
   *   <td><code>00000001</code></td>
   *   <td></td>
   *   <td></td>
   * </tr>
   * <tr valign="bottom">
   *   <td>2</td>
   *   <td><code>00000010</code></td>
   *   <td></td>
   *   <td></td>
   * </tr>
   * <tr>
   *   <td valign="top">...</td>
   *   <td valign="bottom"></td>
   *   <td valign="bottom"></td>
   *   <td valign="bottom"></td>
   * </tr>
   * <tr valign="bottom">
   *   <td>127</td>
   *   <td><code>01111111</code></td>
   *   <td></td>
   *   <td></td>
   * </tr>
   * <tr valign="bottom">
   *   <td>128</td>
   *   <td><code>10000000</code></td>
   *   <td><code>00000001</code></td>
   *   <td></td>
   * </tr>
   * <tr valign="bottom">
   *   <td>129</td>
   *   <td><code>10000001</code></td>
   *   <td><code>00000001</code></td>
   *   <td></td>
   * </tr>
   * <tr valign="bottom">
   *   <td>130</td>
   *   <td><code>10000010</code></td>
   *   <td><code>00000001</code></td>
   *   <td></td>
   * </tr>
   * <tr>
   *   <td valign="top">...</td>
   *   <td></td>
   *   <td></td>
   *   <td></td>
   * </tr>
   * <tr valign="bottom">
   *   <td>16,383</td>
   *   <td><code>11111111</code></td>
   *   <td><code>01111111</code></td>
   *   <td></td>
   * </tr>
   * <tr valign="bottom">
   *   <td>16,384</td>
   *   <td><code>10000000</code></td>
   *   <td><code>10000000</code></td>
   *   <td><code>00000001</code></td>
   * </tr>
   * <tr valign="bottom">
   *   <td>16,385</td>
   *   <td><code>10000001</code></td>
   *   <td><code>10000000</code></td>
   *   <td><code>00000001</code></td>
   * </tr>
   * <tr>
   *   <td valign="top">...</td>
   *   <td valign="bottom"></td>
   *   <td valign="bottom"></td>
   *   <td valign="bottom"></td>
   * </tr>
   * </table>
   * <p>This provides compression while still being efficient to decode.</p>
   * 
   * @param i Smaller values take fewer bytes.  Negative numbers are
   * supported, but should be avoided.
   * @throws IOException If there is an I/O error writing to the underlying medium.
   * @see DataInput#readVInt()
   */
  public final void writeVInt(int i) throws IOException {
    while ((i & ~0x7F) != 0) {
      writeByte((byte)((i & 0x7F) | 0x80));
      i >>>= 7;
    }
    writeByte((byte)i);
  }

  /**
   * Write a {@link BitUtil#zigZagEncode(int) zig-zag}-encoded
   * {@link #writeVInt(int) variable-length} integer. This is typically useful
   * to write small signed ints and is equivalent to calling
   * <code>writeVInt(BitUtil.zigZagEncode(i))</code>.
   * @see DataInput#readZInt()
   */
  public final void writeZInt(int i) throws IOException {
    writeVInt(BitUtil.zigZagEncode(i));
  }

  /** Writes a long as eight bytes.
   * <p>
   * 64-bit unsigned integer written as eight bytes, high-order bytes first.
   * 
   * @see DataInput#readLong()
   */
  public void writeLong(long i) throws IOException {
    writeInt((int) (i >> 32));
    writeInt((int) i);
  }

  /** Writes an long in a variable-length format.  Writes between one and nine
   * bytes.  Smaller values take fewer bytes.  Negative numbers are not
   * supported.
   * <p>
   * The format is described further in {@link DataOutput#writeVInt(int)}.
   * @see DataInput#readVLong()
   */
  public final void writeVLong(long i) throws IOException {
    if (i < 0) {
      throw new IllegalArgumentException("cannot write negative vLong (got: " + i + ")");
    }
    writeSignedVLong(i);
  }

  // write a potentially negative vLong
  private void writeSignedVLong(long i) throws IOException {
    while ((i & ~0x7FL) != 0L) {
      writeByte((byte)((i & 0x7FL) | 0x80L));
      i >>>= 7;
    }
    writeByte((byte)i);
  }

  /**
   * Write a {@link BitUtil#zigZagEncode(long) zig-zag}-encoded
   * {@link #writeVLong(long) variable-length} long. Writes between one and ten
   * bytes. This is typically useful to write small signed ints.
   * @see DataInput#readZLong()
   */
  public final void writeZLong(long i) throws IOException {
    writeSignedVLong(BitUtil.zigZagEncode(i));
  }

  /** Writes a string.
   * <p>
   * Writes strings as UTF-8 encoded bytes. First the length, in bytes, is
   * written as a {@link #writeVInt VInt}, followed by the bytes.
   * 
   * @see DataInput#readString()
   */
  public void writeString(String s) throws IOException {
    final BytesRef utf8Result = new BytesRef(s);
    writeVInt(utf8Result.length);
    writeBytes(utf8Result.bytes, utf8Result.offset, utf8Result.length);
  }

  private static int COPY_BUFFER_SIZE = 16384;
  private byte[] copyBuffer;

  /** Copy numBytes bytes from input to ourself. */
  public void copyBytes(DataInput input, long numBytes) throws IOException {
    assert numBytes >= 0: "numBytes=" + numBytes;
    long left = numBytes;
    if (copyBuffer == null)
      copyBuffer = new byte[COPY_BUFFER_SIZE];
    while(left > 0) {
      final int toCopy;
      if (left > COPY_BUFFER_SIZE)
        toCopy = COPY_BUFFER_SIZE;
      else
        toCopy = (int) left;
      input.readBytes(copyBuffer, 0, toCopy);
      writeBytes(copyBuffer, 0, toCopy);
      left -= toCopy;
    }
  }

  /**
   * Writes a String map.
   * <p>
   * First the size is written as an {@link #writeInt(int) Int32},
   * followed by each key-value pair written as two consecutive 
   * {@link #writeString(String) String}s.
   * 
   * @param map Input map. May be null (equivalent to an empty map)
   * @deprecated Use {@link #writeMapOfStrings(Map)} instead.
   */
  @Deprecated
  public void writeStringStringMap(Map<String,String> map) throws IOException {
    if (map == null) {
      writeInt(0);
    } else {
      writeInt(map.size());
      for(final Map.Entry<String, String> entry: map.entrySet()) {
        writeString(entry.getKey());
        writeString(entry.getValue());
      }
    }
  }
  
  /**
   * Writes a String map.
   * <p>
   * First the size is written as an {@link #writeVInt(int) vInt},
   * followed by each key-value pair written as two consecutive 
   * {@link #writeString(String) String}s.
   * 
   * @param map Input map.
   * @throws NullPointerException if {@code map} is null.
   */
  public void writeMapOfStrings(Map<String,String> map) throws IOException {
    writeVInt(map.size());
    for (Map.Entry<String, String> entry : map.entrySet()) {
      writeString(entry.getKey());
      writeString(entry.getValue());
    }
  }

  /**
   * Writes a String set.
   * <p>
   * First the size is written as an {@link #writeInt(int) Int32},
   * followed by each value written as a
   * {@link #writeString(String) String}.
   * 
   * @param set Input set. May be null (equivalent to an empty set)
   * @deprecated Use {@link #writeMapOfStrings(Map)} instead.
   */
  @Deprecated
  public void writeStringSet(Set<String> set) throws IOException {
    if (set == null) {
      writeInt(0);
    } else {
      writeInt(set.size());
      for(String value : set) {
        writeString(value);
      }
    }
  }
  
  /**
   * Writes a String set.
   * <p>
   * First the size is written as an {@link #writeVInt(int) vInt},
   * followed by each value written as a
   * {@link #writeString(String) String}.
   * 
   * @param set Input set.
   * @throws NullPointerException if {@code set} is null.
   */
  public void writeSetOfStrings(Set<String> set) throws IOException {
    writeVInt(set.size());
    for (String value : set) {
      writeString(value);
    }
  }
}

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
package org.apache.lucene5_shaded.codecs.compressing;


import java.io.IOException;

import org.apache.lucene5_shaded.store.DataOutput;
import org.apache.lucene5_shaded.util.ArrayUtil;
import org.apache.lucene5_shaded.util.UnicodeUtil;

/**
 * A {@link DataOutput} that can be used to build a byte[].
 * @lucene.internal
 */
public final class GrowableByteArrayDataOutput extends DataOutput {

  /** Minimum utf8 byte size of a string over which double pass over string is to save memory during encode */
  static final int MIN_UTF8_SIZE_TO_ENABLE_DOUBLE_PASS_ENCODING = 65536;

  /** The bytes */
  public byte[] bytes;
  /** The length */
  public int length;

  // scratch for utf8 encoding of small strings
  byte[] scratchBytes = new byte[16];

  /** Create a {@link GrowableByteArrayDataOutput} with the given initial capacity. */
  public GrowableByteArrayDataOutput(int cp) {
    this.bytes = new byte[ArrayUtil.oversize(cp, 1)];
    this.length = 0;
  }

  @Override
  public void writeByte(byte b) {
    if (length >= bytes.length) {
      bytes = ArrayUtil.grow(bytes);
    }
    bytes[length++] = b;
  }

  @Override
  public void writeBytes(byte[] b, int off, int len) {
    final int newLength = length + len;
    bytes = ArrayUtil.grow(bytes, newLength);
    System.arraycopy(b, off, bytes, length, len);
    length = newLength;
  }

  @Override
  public void writeString(String string) throws IOException {
    int maxLen = string.length() * UnicodeUtil.MAX_UTF8_BYTES_PER_CHAR;
    if (maxLen <= MIN_UTF8_SIZE_TO_ENABLE_DOUBLE_PASS_ENCODING)  {
      // string is small enough that we don't need to save memory by falling back to double-pass approach
      // this is just an optimized writeString() that re-uses scratchBytes.
      scratchBytes = ArrayUtil.grow(scratchBytes, maxLen);
      int len = UnicodeUtil.UTF16toUTF8(string, 0, string.length(), scratchBytes);
      writeVInt(len);
      writeBytes(scratchBytes, len);
    } else  {
      // use a double pass approach to avoid allocating a large intermediate buffer for string encoding
      int numBytes = UnicodeUtil.calcUTF16toUTF8Length(string, 0, string.length());
      writeVInt(numBytes);
      bytes = ArrayUtil.grow(bytes, length + numBytes);
      length = UnicodeUtil.UTF16toUTF8(string, 0, string.length(), bytes, length);
    }
  }
}

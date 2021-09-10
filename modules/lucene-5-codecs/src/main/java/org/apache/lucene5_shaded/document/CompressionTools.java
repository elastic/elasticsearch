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
package org.apache.lucene5_shaded.document;


import java.util.zip.Deflater;
import java.util.zip.Inflater;
import java.util.zip.DataFormatException;
import java.io.ByteArrayOutputStream;

import org.apache.lucene5_shaded.util.BytesRef;
import org.apache.lucene5_shaded.util.UnicodeUtil;

/** Simple utility class providing static methods to
 *  compress and decompress binary data for stored fields.
 *  This class uses java.util.zip.Deflater and Inflater
 *  classes to compress and decompress.
 */

public class CompressionTools {

  // Export only static methods
  private CompressionTools() {}

  /** Compresses the specified byte range using the
   *  specified compressionLevel (constants are defined in
   *  java.util.zip.Deflater). */
  public static byte[] compress(byte[] value, int offset, int length, int compressionLevel) {

    /* Create an expandable byte array to hold the compressed data.
     * You cannot use an array that's the same size as the orginal because
     * there is no guarantee that the compressed data will be smaller than
     * the uncompressed data. */
    ByteArrayOutputStream bos = new ByteArrayOutputStream(length);

    Deflater compressor = new Deflater();

    try {
      compressor.setLevel(compressionLevel);
      compressor.setInput(value, offset, length);
      compressor.finish();

      // Compress the data
      final byte[] buf = new byte[1024];
      while (!compressor.finished()) {
        int count = compressor.deflate(buf);
        bos.write(buf, 0, count);
      }
    } finally {
      compressor.end();
    }

    return bos.toByteArray();
  }

  /** Compresses the specified byte range, with default BEST_COMPRESSION level */
  public static byte[] compress(byte[] value, int offset, int length) {
    return compress(value, offset, length, Deflater.BEST_COMPRESSION);
  }
  
  /** Compresses all bytes in the array, with default BEST_COMPRESSION level */
  public static byte[] compress(byte[] value) {
    return compress(value, 0, value.length, Deflater.BEST_COMPRESSION);
  }

  /** Compresses the String value, with default BEST_COMPRESSION level */
  public static byte[] compressString(String value) {
    return compressString(value, Deflater.BEST_COMPRESSION);
  }

  /** Compresses the String value using the specified
   *  compressionLevel (constants are defined in
   *  java.util.zip.Deflater). */
  public static byte[] compressString(String value, int compressionLevel) {
    byte[] b = new byte[UnicodeUtil.MAX_UTF8_BYTES_PER_CHAR * value.length()];
    final int len = UnicodeUtil.UTF16toUTF8(value, 0, value.length(), b);
    return compress(b, 0, len, compressionLevel);
  }

  /** Decompress the byte array previously returned by
   *  compress (referenced by the provided BytesRef) */
  public static byte[] decompress(BytesRef bytes) throws DataFormatException {
    return decompress(bytes.bytes, bytes.offset, bytes.length);
  }

  /** Decompress the byte array previously returned by
   *  compress */
  public static byte[] decompress(byte[] value) throws DataFormatException {
    return decompress(value, 0, value.length);
  }

  /** Decompress the byte array previously returned by
   *  compress */
  public static byte[] decompress(byte[] value, int offset, int length) throws DataFormatException {
    // Create an expandable byte array to hold the decompressed data
    ByteArrayOutputStream bos = new ByteArrayOutputStream(length);

    Inflater decompressor = new Inflater();

    try {
      decompressor.setInput(value, offset, length);

      // Decompress the data
      final byte[] buf = new byte[1024];
      while (!decompressor.finished()) {
        int count = decompressor.inflate(buf);
        bos.write(buf, 0, count);
      }
    } finally {  
      decompressor.end();
    }
    
    return bos.toByteArray();
  }

  /** Decompress the byte array previously returned by
   *  compressString back into a String */
  public static String decompressString(byte[] value) throws DataFormatException {
    return decompressString(value, 0, value.length);
  }

  /** Decompress the byte array previously returned by
   *  compressString back into a String */
  public static String decompressString(byte[] value, int offset, int length) throws DataFormatException {
    final byte[] bytes = decompress(value, offset, length);
    final char[] result = new char[bytes.length];
    final int len = UnicodeUtil.UTF8toUTF16(bytes, 0, bytes.length, result);
    return new String(result, 0, len);
  }

  /** Decompress the byte array (referenced by the provided BytesRef) 
   *  previously returned by compressString back into a String */
  public static String decompressString(BytesRef bytes) throws DataFormatException {
    return decompressString(bytes.bytes, bytes.offset, bytes.length);
  }
}

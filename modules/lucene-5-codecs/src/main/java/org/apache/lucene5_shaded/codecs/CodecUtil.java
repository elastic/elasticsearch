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
package org.apache.lucene5_shaded.codecs;


import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import org.apache.lucene5_shaded.index.CorruptIndexException;
import org.apache.lucene5_shaded.index.IndexFormatTooNewException;
import org.apache.lucene5_shaded.index.IndexFormatTooOldException;
import org.apache.lucene5_shaded.store.BufferedChecksumIndexInput;
import org.apache.lucene5_shaded.store.ChecksumIndexInput;
import org.apache.lucene5_shaded.store.DataInput;
import org.apache.lucene5_shaded.store.DataOutput;
import org.apache.lucene5_shaded.store.IndexInput;
import org.apache.lucene5_shaded.store.IndexOutput;
import org.apache.lucene5_shaded.util.BytesRef;
import org.apache.lucene5_shaded.util.IOUtils;
import org.apache.lucene5_shaded.util.StringHelper;

/**
 * Utility class for reading and writing versioned headers.
 * <p>
 * Writing codec headers is useful to ensure that a file is in 
 * the format you think it is.
 * 
 * @lucene.experimental
 */

public final class CodecUtil {
  private CodecUtil() {} // no instance

  /**
   * Constant to identify the start of a codec header.
   */
  public final static int CODEC_MAGIC = 0x3fd76c17;
  /**
   * Constant to identify the start of a codec footer.
   */
  public final static int FOOTER_MAGIC = ~CODEC_MAGIC;

  /**
   * Writes a codec header, which records both a string to
   * identify the file and a version number. This header can
   * be parsed and validated with 
   * {@link #checkHeader(DataInput, String, int, int) checkHeader()}.
   * <p>
   * CodecHeader --&gt; Magic,CodecName,Version
   * <ul>
   *    <li>Magic --&gt; {@link DataOutput#writeInt Uint32}. This
   *        identifies the start of the header. It is always {@value #CODEC_MAGIC}.
   *    <li>CodecName --&gt; {@link DataOutput#writeString String}. This
   *        is a string to identify this file.
   *    <li>Version --&gt; {@link DataOutput#writeInt Uint32}. Records
   *        the version of the file.
   * </ul>
   * <p>
   * Note that the length of a codec header depends only upon the
   * name of the codec, so this length can be computed at any time
   * with {@link #headerLength(String)}.
   * 
   * @param out Output stream
   * @param codec String to identify this file. It should be simple ASCII, 
   *              less than 128 characters in length.
   * @param version Version number
   * @throws IOException If there is an I/O error writing to the underlying medium.
   * @throws IllegalArgumentException If the codec name is not simple ASCII, or is more than 127 characters in length
   */
  public static void writeHeader(DataOutput out, String codec, int version) throws IOException {
    BytesRef bytes = new BytesRef(codec);
    if (bytes.length != codec.length() || bytes.length >= 128) {
      throw new IllegalArgumentException("codec must be simple ASCII, less than 128 characters in length [got " + codec + "]");
    }
    out.writeInt(CODEC_MAGIC);
    out.writeString(codec);
    out.writeInt(version);
  }
  
  /**
   * Writes a codec header for an index file, which records both a string to
   * identify the format of the file, a version number, and data to identify
   * the file instance (ID and auxiliary suffix such as generation).
   * <p>
   * This header can be parsed and validated with 
   * {@link #checkIndexHeader(DataInput, String, int, int, byte[], String) checkIndexHeader()}.
   * <p>
   * IndexHeader --&gt; CodecHeader,ObjectID,ObjectSuffix
   * <ul>
   *    <li>CodecHeader   --&gt; {@link #writeHeader}
   *    <li>ObjectID     --&gt; {@link DataOutput#writeByte byte}<sup>16</sup>
   *    <li>ObjectSuffix --&gt; SuffixLength,SuffixBytes
   *    <li>SuffixLength  --&gt; {@link DataOutput#writeByte byte}
   *    <li>SuffixBytes   --&gt; {@link DataOutput#writeByte byte}<sup>SuffixLength</sup>
   * </ul>
   * <p>
   * Note that the length of an index header depends only upon the
   * name of the codec and suffix, so this length can be computed at any time
   * with {@link #indexHeaderLength(String,String)}.
   * 
   * @param out Output stream
   * @param codec String to identify the format of this file. It should be simple ASCII, 
   *              less than 128 characters in length.
   * @param id Unique identifier for this particular file instance.
   * @param suffix auxiliary suffix information for the file. It should be simple ASCII,
   *              less than 256 characters in length.
   * @param version Version number
   * @throws IOException If there is an I/O error writing to the underlying medium.
   * @throws IllegalArgumentException If the codec name is not simple ASCII, or 
   *         is more than 127 characters in length, or if id is invalid,
   *         or if the suffix is not simple ASCII, or more than 255 characters
   *         in length.
   */
  public static void writeIndexHeader(DataOutput out, String codec, int version, byte[] id, String suffix) throws IOException {
    if (id.length != StringHelper.ID_LENGTH) {
      throw new IllegalArgumentException("Invalid id: " + StringHelper.idToString(id));
    }
    writeHeader(out, codec, version);
    out.writeBytes(id, 0, id.length);
    BytesRef suffixBytes = new BytesRef(suffix);
    if (suffixBytes.length != suffix.length() || suffixBytes.length >= 256) {
      throw new IllegalArgumentException("suffix must be simple ASCII, less than 256 characters in length [got " + suffix + "]");
    }
    out.writeByte((byte) suffixBytes.length);
    out.writeBytes(suffixBytes.bytes, suffixBytes.offset, suffixBytes.length);
  }

  /**
   * Computes the length of a codec header.
   * 
   * @param codec Codec name.
   * @return length of the entire codec header.
   * @see #writeHeader(DataOutput, String, int)
   */
  public static int headerLength(String codec) {
    return 9+codec.length();
  }
  
  /**
   * Computes the length of an index header.
   * 
   * @param codec Codec name.
   * @return length of the entire index header.
   * @see #writeIndexHeader(DataOutput, String, int, byte[], String)
   */
  public static int indexHeaderLength(String codec, String suffix) {
    return headerLength(codec) + StringHelper.ID_LENGTH + 1 + suffix.length();
  }

  /**
   * Reads and validates a header previously written with 
   * {@link #writeHeader(DataOutput, String, int)}.
   * <p>
   * When reading a file, supply the expected <code>codec</code> and
   * an expected version range (<code>minVersion to maxVersion</code>).
   * 
   * @param in Input stream, positioned at the point where the
   *        header was previously written. Typically this is located
   *        at the beginning of the file.
   * @param codec The expected codec name.
   * @param minVersion The minimum supported expected version number.
   * @param maxVersion The maximum supported expected version number.
   * @return The actual version found, when a valid header is found 
   *         that matches <code>codec</code>, with an actual version 
   *         where {@code minVersion <= actual <= maxVersion}.
   *         Otherwise an exception is thrown.
   * @throws CorruptIndexException If the first four bytes are not
   *         {@link #CODEC_MAGIC}, or if the actual codec found is
   *         not <code>codec</code>.
   * @throws IndexFormatTooOldException If the actual version is less 
   *         than <code>minVersion</code>.
   * @throws IndexFormatTooNewException If the actual version is greater 
   *         than <code>maxVersion</code>.
   * @throws IOException If there is an I/O error reading from the underlying medium.
   * @see #writeHeader(DataOutput, String, int)
   */
  public static int checkHeader(DataInput in, String codec, int minVersion, int maxVersion) throws IOException {
    // Safety to guard against reading a bogus string:
    final int actualHeader = in.readInt();
    if (actualHeader != CODEC_MAGIC) {
      throw new CorruptIndexException("codec header mismatch: actual header=" + actualHeader + " vs expected header=" + CODEC_MAGIC, in);
    }
    return checkHeaderNoMagic(in, codec, minVersion, maxVersion);
  }

  /** Like {@link
   *  #checkHeader(DataInput,String,int,int)} except this
   *  version assumes the first int has already been read
   *  and validated from the input. */
  public static int checkHeaderNoMagic(DataInput in, String codec, int minVersion, int maxVersion) throws IOException {
    final String actualCodec = in.readString();
    if (!actualCodec.equals(codec)) {
      throw new CorruptIndexException("codec mismatch: actual codec=" + actualCodec + " vs expected codec=" + codec, in);
    }

    final int actualVersion = in.readInt();
    if (actualVersion < minVersion) {
      throw new IndexFormatTooOldException(in, actualVersion, minVersion, maxVersion);
    }
    if (actualVersion > maxVersion) {
      throw new IndexFormatTooNewException(in, actualVersion, minVersion, maxVersion);
    }

    return actualVersion;
  }
  
  /**
   * Reads and validates a header previously written with 
   * {@link #writeIndexHeader(DataOutput, String, int, byte[], String)}.
   * <p>
   * When reading a file, supply the expected <code>codec</code>,
   * expected version range (<code>minVersion to maxVersion</code>),
   * and object ID and suffix.
   * 
   * @param in Input stream, positioned at the point where the
   *        header was previously written. Typically this is located
   *        at the beginning of the file.
   * @param codec The expected codec name.
   * @param minVersion The minimum supported expected version number.
   * @param maxVersion The maximum supported expected version number.
   * @param expectedID The expected object identifier for this file.
   * @param expectedSuffix The expected auxiliary suffix for this file.
   * @return The actual version found, when a valid header is found 
   *         that matches <code>codec</code>, with an actual version 
   *         where {@code minVersion <= actual <= maxVersion}, 
   *         and matching <code>expectedID</code> and <code>expectedSuffix</code>
   *         Otherwise an exception is thrown.
   * @throws CorruptIndexException If the first four bytes are not
   *         {@link #CODEC_MAGIC}, or if the actual codec found is
   *         not <code>codec</code>, or if the <code>expectedID</code>
   *         or <code>expectedSuffix</code> do not match.
   * @throws IndexFormatTooOldException If the actual version is less 
   *         than <code>minVersion</code>.
   * @throws IndexFormatTooNewException If the actual version is greater 
   *         than <code>maxVersion</code>.
   * @throws IOException If there is an I/O error reading from the underlying medium.
   * @see #writeIndexHeader(DataOutput, String, int, byte[],String)
   */
  public static int checkIndexHeader(DataInput in, String codec, int minVersion, int maxVersion, byte[] expectedID, String expectedSuffix) throws IOException {
    int version = checkHeader(in, codec, minVersion, maxVersion);
    checkIndexHeaderID(in, expectedID);
    checkIndexHeaderSuffix(in, expectedSuffix);
    return version;
  }
  
  /** Expert: just reads and verifies the object ID of an index header */
  public static byte[] checkIndexHeaderID(DataInput in, byte[] expectedID) throws IOException {
    byte id[] = new byte[StringHelper.ID_LENGTH];
    in.readBytes(id, 0, id.length);
    if (!Arrays.equals(id, expectedID)) {
      throw new CorruptIndexException("file mismatch, expected id=" + StringHelper.idToString(expectedID) 
                                                         + ", got=" + StringHelper.idToString(id), in);
    }
    return id;
  }
  
  /** Expert: just reads and verifies the suffix of an index header */
  public static String checkIndexHeaderSuffix(DataInput in, String expectedSuffix) throws IOException {
    int suffixLength = in.readByte() & 0xFF;
    byte suffixBytes[] = new byte[suffixLength];
    in.readBytes(suffixBytes, 0, suffixBytes.length);
    String suffix = new String(suffixBytes, 0, suffixBytes.length, StandardCharsets.UTF_8);
    if (!suffix.equals(expectedSuffix)) {
      throw new CorruptIndexException("file mismatch, expected suffix=" + expectedSuffix
                                                             + ", got=" + suffix, in);
    }
    return suffix;
  }
  
  /**
   * Writes a codec footer, which records both a checksum
   * algorithm ID and a checksum. This footer can
   * be parsed and validated with 
   * {@link #checkFooter(ChecksumIndexInput) checkFooter()}.
   * <p>
   * CodecFooter --&gt; Magic,AlgorithmID,Checksum
   * <ul>
   *    <li>Magic --&gt; {@link DataOutput#writeInt Uint32}. This
   *        identifies the start of the footer. It is always {@value #FOOTER_MAGIC}.
   *    <li>AlgorithmID --&gt; {@link DataOutput#writeInt Uint32}. This
   *        indicates the checksum algorithm used. Currently this is always 0,
   *        for zlib-crc32.
   *    <li>Checksum --&gt; {@link DataOutput#writeLong Uint64}. The
   *        actual checksum value for all previous bytes in the stream, including
   *        the bytes from Magic and AlgorithmID.
   * </ul>
   * 
   * @param out Output stream
   * @throws IOException If there is an I/O error writing to the underlying medium.
   */
  public static void writeFooter(IndexOutput out) throws IOException {
    out.writeInt(FOOTER_MAGIC);
    out.writeInt(0);
    writeCRC(out);
  }
  
  /**
   * Computes the length of a codec footer.
   * 
   * @return length of the entire codec footer.
   * @see #writeFooter(IndexOutput)
   */
  public static int footerLength() {
    return 16;
  }
  
  /** 
   * Validates the codec footer previously written by {@link #writeFooter}. 
   * @return actual checksum value
   * @throws IOException if the footer is invalid, if the checksum does not match, 
   *                     or if {@code in} is not properly positioned before the footer
   *                     at the end of the stream.
   */
  public static long checkFooter(ChecksumIndexInput in) throws IOException {
    validateFooter(in);
    long actualChecksum = in.getChecksum();
    long expectedChecksum = readCRC(in);
    if (expectedChecksum != actualChecksum) {
      throw new CorruptIndexException("checksum failed (hardware problem?) : expected=" + Long.toHexString(expectedChecksum) +  
                                                       " actual=" + Long.toHexString(actualChecksum), in);
    }
    return actualChecksum;
  }
  
  /** 
   * Validates the codec footer previously written by {@link #writeFooter}, optionally
   * passing an unexpected exception that has already occurred.
   * <p>
   * When a {@code priorException} is provided, this method will add a suppressed exception 
   * indicating whether the checksum for the stream passes, fails, or cannot be computed, and 
   * rethrow it. Otherwise it behaves the same as {@link #checkFooter(ChecksumIndexInput)}.
   * <p>
   * Example usage:
   * <pre class="prettyprint">
   * try (ChecksumIndexInput input = ...) {
   *   Throwable priorE = null;
   *   try {
   *     // ... read a bunch of stuff ... 
   *   } catch (Throwable exception) {
   *     priorE = exception;
   *   } finally {
   *     CodecUtil.checkFooter(input, priorE);
   *   }
   * }
   * </pre>
   */
  public static void checkFooter(ChecksumIndexInput in, Throwable priorException) throws IOException {
    if (priorException == null) {
      checkFooter(in);
    } else {
      try {
        long remaining = in.length() - in.getFilePointer();
        if (remaining < footerLength()) {
          // corruption caused us to read into the checksum footer already: we can't proceed
          priorException.addSuppressed(new CorruptIndexException("checksum status indeterminate: remaining=" + remaining +
                                                                 ", please run checkindex for more details", in));
        } else {
          // otherwise, skip any unread bytes.
          in.skipBytes(remaining - footerLength());
          
          // now check the footer
          try {
            long checksum = checkFooter(in);
            priorException.addSuppressed(new CorruptIndexException("checksum passed (" + Long.toHexString(checksum) + 
                                                                   "). possibly transient resource issue, or a Lucene or JVM bug", in));
          } catch (CorruptIndexException t) {
            priorException.addSuppressed(t);
          }
        }
      } catch (Throwable t) {
        // catch-all for things that shouldn't go wrong (e.g. OOM during readInt) but could...
        priorException.addSuppressed(new CorruptIndexException("checksum status indeterminate: unexpected exception", in, t));
      }
      IOUtils.reThrow(priorException);
    }
  }
  
  /** 
   * Returns (but does not validate) the checksum previously written by {@link #checkFooter}.
   * @return actual checksum value
   * @throws IOException if the footer is invalid
   */
  public static long retrieveChecksum(IndexInput in) throws IOException {
    if (in.length() < footerLength()) {
      throw new CorruptIndexException("misplaced codec footer (file truncated?): length=" + in.length() + " but footerLength==" + footerLength(), in);
    }
    in.seek(in.length() - footerLength());
    validateFooter(in);
    return readCRC(in);
  }
  
  private static void validateFooter(IndexInput in) throws IOException {
    long remaining = in.length() - in.getFilePointer();
    long expected = footerLength();
    if (remaining < expected) {
      throw new CorruptIndexException("misplaced codec footer (file truncated?): remaining=" + remaining + ", expected=" + expected, in);
    } else if (remaining > expected) {
      throw new CorruptIndexException("misplaced codec footer (file extended?): remaining=" + remaining + ", expected=" + expected, in);
    }
    
    final int magic = in.readInt();
    if (magic != FOOTER_MAGIC) {
      throw new CorruptIndexException("codec footer mismatch (file truncated?): actual footer=" + magic + " vs expected footer=" + FOOTER_MAGIC, in);
    }
    
    final int algorithmID = in.readInt();
    if (algorithmID != 0) {
      throw new CorruptIndexException("codec footer mismatch: unknown algorithmID: " + algorithmID, in);
    }
  }
  
  /**
   * Checks that the stream is positioned at the end, and throws exception
   * if it is not. 
   * @deprecated Use {@link #checkFooter} instead, this should only used for files without checksums 
   */
  @Deprecated
  public static void checkEOF(IndexInput in) throws IOException {
    if (in.getFilePointer() != in.length()) {
      throw new CorruptIndexException("did not read all bytes from file: read " + in.getFilePointer() + " vs size " + in.length(), in);
    }
  }
  
  /** 
   * Clones the provided input, reads all bytes from the file, and calls {@link #checkFooter} 
   * <p>
   * Note that this method may be slow, as it must process the entire file.
   * If you just need to extract the checksum value, call {@link #retrieveChecksum}.
   */
  public static long checksumEntireFile(IndexInput input) throws IOException {
    IndexInput clone = input.clone();
    clone.seek(0);
    ChecksumIndexInput in = new BufferedChecksumIndexInput(clone);
    assert in.getFilePointer() == 0;
    in.seek(in.length() - footerLength());
    return checkFooter(in);
  }
  
  /**
   * Reads CRC32 value as a 64-bit long from the input.
   * @throws CorruptIndexException if CRC is formatted incorrectly (wrong bits set)
   * @throws IOException if an i/o error occurs
   */
  public static long readCRC(IndexInput input) throws IOException {
    long value = input.readLong();
    if ((value & 0xFFFFFFFF00000000L) != 0) {
      throw new CorruptIndexException("Illegal CRC-32 checksum: " + value, input);
    }
    return value;
  }
  
  /**
   * Writes CRC32 value as a 64-bit long to the output.
   * @throws IllegalStateException if CRC is formatted incorrectly (wrong bits set)
   * @throws IOException if an i/o error occurs
   */
  public static void writeCRC(IndexOutput output) throws IOException {
    long value = output.getChecksum();
    if ((value & 0xFFFFFFFF00000000L) != 0) {
      throw new IllegalStateException("Illegal CRC-32 checksum: " + value + " (resource=" + output + ")");
    }
    output.writeLong(value);
  }
}

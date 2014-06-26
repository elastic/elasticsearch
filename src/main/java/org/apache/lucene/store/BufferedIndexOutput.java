/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.lucene.store;

import java.io.IOException;
import java.util.zip.CRC32;

/** Base implementation class for buffered {@link IndexOutput}. */
public abstract class BufferedIndexOutput extends IndexOutput {
  /** The default buffer size in bytes ({@value #DEFAULT_BUFFER_SIZE}). */
  public static final int DEFAULT_BUFFER_SIZE = 16384;

  private final int bufferSize;
  private final byte[] buffer;
  private long bufferStart = 0;           // position in file of buffer
  private int bufferPosition = 0;         // position in buffer
  private final CRC32 crc = new CRC32();

  /**
   * Creates a new {@link BufferedIndexOutput} with the default buffer size
   * ({@value #DEFAULT_BUFFER_SIZE} bytes see {@link #DEFAULT_BUFFER_SIZE})
   */
  public BufferedIndexOutput() {
    this(DEFAULT_BUFFER_SIZE);
  }
  
  /**
   * Creates a new {@link BufferedIndexOutput} with the given buffer size. 
   * @param bufferSize the buffer size in bytes used to buffer writes internally.
   * @throws IllegalArgumentException if the given buffer size is less or equal to <tt>0</tt>
   */
  public BufferedIndexOutput(int bufferSize) {
    if (bufferSize <= 0) {
      throw new IllegalArgumentException("bufferSize must be greater than 0 (got " + bufferSize + ")");
    }
    this.bufferSize = bufferSize;
    buffer = new byte[bufferSize];
  }

  @Override
  public void writeByte(byte b) throws IOException {
    if (bufferPosition >= bufferSize)
      flush();
    buffer[bufferPosition++] = b;
  }

  @Override
  public void writeBytes(byte[] b, int offset, int length) throws IOException {
    int bytesLeft = bufferSize - bufferPosition;
    // is there enough space in the buffer?
    if (bytesLeft >= length) {
      // we add the data to the end of the buffer
      System.arraycopy(b, offset, buffer, bufferPosition, length);
      bufferPosition += length;
      // if the buffer is full, flush it
      if (bufferSize - bufferPosition == 0)
        flush();
    } else {
      // is data larger then buffer?
      if (length > bufferSize) {
        // we flush the buffer
        if (bufferPosition > 0)
          flush();
        // and write data at once
        crc.update(b, offset, length);
        flushBuffer(b, offset, length);
        bufferStart += length;
      } else {
        // we fill/flush the buffer (until the input is written)
        int pos = 0; // position in the input data
        int pieceLength;
        while (pos < length) {
          pieceLength = (length - pos < bytesLeft) ? length - pos : bytesLeft;
          System.arraycopy(b, pos + offset, buffer, bufferPosition, pieceLength);
          pos += pieceLength;
          bufferPosition += pieceLength;
          // if the buffer is full, flush it
          bytesLeft = bufferSize - bufferPosition;
          if (bytesLeft == 0) {
            flush();
            bytesLeft = bufferSize;
          }
        }
      }
    }
  }

  @Override
  public void flush() throws IOException {
    crc.update(buffer, 0, bufferPosition);
    flushBuffer(buffer, bufferPosition);
    bufferStart += bufferPosition;
    bufferPosition = 0;
  }

  /** Expert: implements buffer write.  Writes bytes at the current position in
   * the output.
   * @param b the bytes to write
   * @param len the number of bytes to write
   */
  private void flushBuffer(byte[] b, int len) throws IOException {
    flushBuffer(b, 0, len);
  }

  /** Expert: implements buffer write.  Writes bytes at the current position in
   * the output.
   * @param b the bytes to write
   * @param offset the offset in the byte array
   * @param len the number of bytes to write
   */
  protected abstract void flushBuffer(byte[] b, int offset, int len) throws IOException;
  
  @Override
  public void close() throws IOException {
    flush();
  }

  @Override
  public long getFilePointer() {
    return bufferStart + bufferPosition;
  }

  @Override
  public abstract long length() throws IOException;
  
  /**
   * Returns size of the used output buffer in bytes.
   * */
  public final int getBufferSize() {
    return bufferSize;
  }

  @Override
  public long getChecksum() throws IOException {
    flush();
    return crc.getValue();
  }
}

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
package org.apache.lucene5_shaded.index;


import java.io.IOException;

import org.apache.lucene5_shaded.store.DataInput;
import org.apache.lucene5_shaded.store.DataOutput;
import org.apache.lucene5_shaded.util.ByteBlockPool;

/* IndexInput that knows how to read the byte slices written
 * by Posting and PostingVector.  We read the bytes in
 * each slice until we hit the end of that slice at which
 * point we read the forwarding address of the next slice
 * and then jump to it.*/
final class ByteSliceReader extends DataInput {
  ByteBlockPool pool;
  int bufferUpto;
  byte[] buffer;
  public int upto;
  int limit;
  int level;
  public int bufferOffset;

  public int endIndex;

  public void init(ByteBlockPool pool, int startIndex, int endIndex) {

    assert endIndex-startIndex >= 0;
    assert startIndex >= 0;
    assert endIndex >= 0;

    this.pool = pool;
    this.endIndex = endIndex;

    level = 0;
    bufferUpto = startIndex / ByteBlockPool.BYTE_BLOCK_SIZE;
    bufferOffset = bufferUpto * ByteBlockPool.BYTE_BLOCK_SIZE;
    buffer = pool.buffers[bufferUpto];
    upto = startIndex & ByteBlockPool.BYTE_BLOCK_MASK;

    final int firstSize = ByteBlockPool.LEVEL_SIZE_ARRAY[0];

    if (startIndex+firstSize >= endIndex) {
      // There is only this one slice to read
      limit = endIndex & ByteBlockPool.BYTE_BLOCK_MASK;
    } else
      limit = upto+firstSize-4;
  }

  public boolean eof() {
    assert upto + bufferOffset <= endIndex;
    return upto + bufferOffset == endIndex;
  }

  @Override
  public byte readByte() {
    assert !eof();
    assert upto <= limit;
    if (upto == limit)
      nextSlice();
    return buffer[upto++];
  }

  public long writeTo(DataOutput out) throws IOException {
    long size = 0;
    while(true) {
      if (limit + bufferOffset == endIndex) {
        assert endIndex - bufferOffset >= upto;
        out.writeBytes(buffer, upto, limit-upto);
        size += limit-upto;
        break;
      } else {
        out.writeBytes(buffer, upto, limit-upto);
        size += limit-upto;
        nextSlice();
      }
    }

    return size;
  }

  public void nextSlice() {

    // Skip to our next slice
    final int nextIndex = ((buffer[limit]&0xff)<<24) + ((buffer[1+limit]&0xff)<<16) + ((buffer[2+limit]&0xff)<<8) + (buffer[3+limit]&0xff);

    level = ByteBlockPool.NEXT_LEVEL_ARRAY[level];
    final int newSize = ByteBlockPool.LEVEL_SIZE_ARRAY[level];

    bufferUpto = nextIndex / ByteBlockPool.BYTE_BLOCK_SIZE;
    bufferOffset = bufferUpto * ByteBlockPool.BYTE_BLOCK_SIZE;

    buffer = pool.buffers[bufferUpto];
    upto = nextIndex & ByteBlockPool.BYTE_BLOCK_MASK;

    if (nextIndex + newSize >= endIndex) {
      // We are advancing to the final slice
      assert endIndex - nextIndex > 0;
      limit = endIndex - bufferOffset;
    } else {
      // This is not the final slice (subtract 4 for the
      // forwarding address at the end of this new slice)
      limit = upto+newSize-4;
    }
  }

  @Override
  public void readBytes(byte[] b, int offset, int len) {
    while(len > 0) {
      final int numLeft = limit-upto;
      if (numLeft < len) {
        // Read entire slice
        System.arraycopy(buffer, upto, b, offset, numLeft);
        offset += numLeft;
        len -= numLeft;
        nextSlice();
      } else {
        // This slice is the last one
        System.arraycopy(buffer, upto, b, offset, len);
        upto += len;
        break;
      }
    }
  }
}
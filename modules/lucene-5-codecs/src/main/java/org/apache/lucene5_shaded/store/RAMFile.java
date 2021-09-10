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


import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Arrays;

import org.apache.lucene5_shaded.util.Accountable;

/** 
 * Represents a file in RAM as a list of byte[] buffers.
 * @lucene.internal */
public class RAMFile implements Accountable {
  protected final ArrayList<byte[]> buffers = new ArrayList<>();
  long length;
  RAMDirectory directory;
  protected long sizeInBytes;

  // File used as buffer, in no RAMDirectory
  public RAMFile() {}
  
  RAMFile(RAMDirectory directory) {
    this.directory = directory;
  }

  // For non-stream access from thread that might be concurrent with writing
  public synchronized long getLength() {
    return length;
  }

  protected synchronized void setLength(long length) {
    this.length = length;
  }

  protected final byte[] addBuffer(int size) {
    byte[] buffer = newBuffer(size);
    synchronized(this) {
      buffers.add(buffer);
      sizeInBytes += size;
    }

    if (directory != null) {
      directory.sizeInBytes.getAndAdd(size);
    }
    return buffer;
  }

  protected final synchronized byte[] getBuffer(int index) {
    return buffers.get(index);
  }

  protected final synchronized int numBuffers() {
    return buffers.size();
  }

  /**
   * Expert: allocate a new buffer. 
   * Subclasses can allocate differently. 
   * @param size size of allocated buffer.
   * @return allocated buffer.
   */
  protected byte[] newBuffer(int size) {
    return new byte[size];
  }

  @Override
  public synchronized long ramBytesUsed() {
    return sizeInBytes;
  }
  
  @Override
  public Collection<Accountable> getChildResources() {
    return Collections.emptyList();
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "(length=" + length + ")";
  }

  @Override
  public int hashCode() {
    int h = (int) (length ^ (length >>> 32));
    for (byte[] block : buffers) {
      h = 31 * h + Arrays.hashCode(block);
    }
    return h;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null) return false;
    if (getClass() != obj.getClass()) return false;
    RAMFile other = (RAMFile) obj;
    if (length != other.length) return false;
    if (buffers.size() != other.buffers.size()) {
      return false;
    }
    for (int i = 0; i < buffers.size(); i++) {
      if (!Arrays.equals(buffers.get(i), other.buffers.get(i))) {
        return false;
      }
    }
    return true;
  }
}

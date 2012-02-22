package org.apache.lucene.store.bytebuffer;

/**
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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 */
public class ByteBufferFile {

    final ByteBufferDirectory dir;

    final int bufferSize;

    final List<ByteBuffer> buffers;

    long length;

    volatile long lastModified = System.currentTimeMillis();

    final AtomicInteger refCount;

    long sizeInBytes;

    public ByteBufferFile(ByteBufferDirectory dir, int bufferSize) {
        this.dir = dir;
        this.bufferSize = bufferSize;
        this.buffers = new ArrayList<ByteBuffer>();
        this.refCount = new AtomicInteger(1);
    }

    ByteBufferFile(ByteBufferFile file) {
        this.dir = file.dir;
        this.bufferSize = file.bufferSize;
        this.buffers = file.buffers;
        this.length = file.length;
        this.lastModified = file.lastModified;
        this.refCount = file.refCount;
        this.sizeInBytes = file.sizeInBytes;
    }

    public long getLength() {
        return length;
    }

    public long getLastModified() {
        return lastModified;
    }

    void setLastModified(long lastModified) {
        this.lastModified = lastModified;
    }

    long sizeInBytes() {
        return sizeInBytes;
    }

    ByteBuffer getBuffer(int index) {
        return buffers.get(index);
    }

    int numBuffers() {
        return buffers.size();
    }

    void delete() {
        decRef();
    }

    void incRef() {
        refCount.incrementAndGet();
    }

    void decRef() {
        if (refCount.decrementAndGet() == 0) {
            length = 0;
            for (ByteBuffer buffer : buffers) {
                dir.releaseBuffer(buffer);
            }
            buffers.clear();
            sizeInBytes = 0;
        }
    }
}

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
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 */
public class ByteBufferFile {

    private final CopyOnWriteArrayList<ByteBuffer> buffers = new CopyOnWriteArrayList<ByteBuffer>();
    private final ByteBufferDirectory dir;
    final int bufferSize;

    private volatile long length;
    // This is publicly modifiable via Directory.touchFile(), so direct access not supported
    private volatile long lastModified = System.currentTimeMillis();

    private final AtomicInteger refCount = new AtomicInteger(1);

    final AtomicLong sizeInBytes = new AtomicLong();

    public ByteBufferFile(ByteBufferDirectory dir, int bufferSize) {
        this.dir = dir;
        this.bufferSize = bufferSize;
    }

    // For non-stream access from thread that might be concurrent with writing
    public long getLength() {
        return length;
    }

    protected void setLength(long length) {
        this.length = length;
    }

    // For non-stream access from thread that might be concurrent with writing
    public long getLastModified() {
        return lastModified;
    }

    protected void setLastModified(long lastModified) {
        this.lastModified = lastModified;
    }

    protected final void addBuffer(ByteBuffer buffer) {
        buffers.add(buffer);
        sizeInBytes.addAndGet(buffer.remaining());
        dir.sizeInBytes.addAndGet(buffer.remaining());
    }

    protected final ByteBuffer getBuffer(int index) {
        return buffers.get(index);
    }

    protected final int numBuffers() {
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
        }
    }
}

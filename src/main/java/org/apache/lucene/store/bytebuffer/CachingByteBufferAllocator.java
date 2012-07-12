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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * The caching byte buffer allocator allows to define a global size for both the small and large buffers
 * allocated. Those will be reused when possible.
 */
public class CachingByteBufferAllocator extends PlainByteBufferAllocator {

    private final BlockingQueue<ByteBuffer> smallCache;
    private final BlockingQueue<ByteBuffer> largeCache;

    /**
     * @param direct                 If set to true, will allocate direct buffers (off heap).
     * @param smallBufferSizeInBytes The size (in bytes) of the small buffer allocation.
     * @param largeBufferSizeInBytes The size (in bytes) of the large buffer allocation.
     * @param smallCacheSizeInBytes  The size of the small cache buffer in bytes.
     * @param largeCacheSizeInBytes  The size of the large cache buffer in bytes.
     */
    public CachingByteBufferAllocator(boolean direct, int smallBufferSizeInBytes, int largeBufferSizeInBytes,
                                      int smallCacheSizeInBytes, int largeCacheSizeInBytes) {
        super(direct, smallBufferSizeInBytes, largeBufferSizeInBytes);
        this.smallCache = new LinkedBlockingQueue<ByteBuffer>(smallCacheSizeInBytes / smallBufferSizeInBytes);
        this.largeCache = new LinkedBlockingQueue<ByteBuffer>(largeCacheSizeInBytes / largeBufferSizeInBytes);
    }


    public ByteBuffer allocate(Type type) throws IOException {
        ByteBuffer buffer = type == Type.SMALL ? smallCache.poll() : largeCache.poll();
        if (buffer == null) {
            buffer = super.allocate(type);
        }
        return buffer;
    }

    public void release(ByteBuffer buffer) {
        if (buffer.capacity() == smallBufferSizeInBytes) {
            boolean success = smallCache.offer(buffer);
            if (!success) {
                super.release(buffer);
            }
        } else if (buffer.capacity() == largeBufferSizeInBytes) {
            boolean success = largeCache.offer(buffer);
            if (!success) {
                super.release(buffer);
            }
        }
        // otherwise, just ignore it? not our allocation...
    }

    public void close() {
        for (ByteBuffer buffer : smallCache) {
            super.release(buffer);
        }
        smallCache.clear();
        for (ByteBuffer buffer : largeCache) {
            super.release(buffer);
        }
        largeCache.clear();
    }
}

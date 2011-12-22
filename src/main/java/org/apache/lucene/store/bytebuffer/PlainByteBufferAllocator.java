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

/**
 * A simple byte buffer allocator that does not caching. The direct flag
 * allows to control if the byte buffer will be allocated off heap or not.
 */
public class PlainByteBufferAllocator implements ByteBufferAllocator {

    protected final boolean direct;

    protected final int smallBufferSizeInBytes;

    protected final int largeBufferSizeInBytes;

    /**
     * Constructs a new plain byte buffer allocator that does no caching.
     *
     * @param direct                 If set to true, will allocate direct buffers (off heap).
     * @param smallBufferSizeInBytes The size (in bytes) of the small buffer allocation.
     * @param largeBufferSizeInBytes The size (in bytes) of the large buffer allocation.
     */
    public PlainByteBufferAllocator(boolean direct, int smallBufferSizeInBytes, int largeBufferSizeInBytes) {
        this.direct = direct;
        this.smallBufferSizeInBytes = smallBufferSizeInBytes;
        this.largeBufferSizeInBytes = largeBufferSizeInBytes;
    }

    public int sizeInBytes(Type type) {
        return type == Type.SMALL ? smallBufferSizeInBytes : largeBufferSizeInBytes;
    }

    public ByteBuffer allocate(Type type) throws IOException {
        int sizeToAllocate = type == Type.SMALL ? smallBufferSizeInBytes : largeBufferSizeInBytes;
        if (direct) {
            return ByteBuffer.allocateDirect(sizeToAllocate);
        }
        return ByteBuffer.allocate(sizeToAllocate);
    }

    public void release(ByteBuffer buffer) {
        Cleaner.clean(buffer);
    }

    public void close() {
        // nothing to do here...
    }
}

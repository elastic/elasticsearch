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
import java.lang.reflect.Method;
import java.nio.ByteBuffer;

/**
 * A byte buffer allocator simple allocates byte buffers, and handles releasing
 * them. Implementation can include special direct buffer cleaning when releasing
 * a buffer, as well as caching of byte buffers.
 * <p/>
 * <p>There are two types of buffers that can be allocated, small and big. This
 * comes in handy when knowing in advance (more or less) the size of the buffers
 * needed (large files or small), as well as in caching implementations.
 */
public interface ByteBufferAllocator {

    /**
     * Helper class to allocator implementations allowing to clean direct buffers.
     */
    public static class Cleaner {
        public static final boolean CLEAN_SUPPORTED;
        private static final Method directBufferCleaner;
        private static final Method directBufferCleanerClean;

        static {
            Method directBufferCleanerX = null;
            Method directBufferCleanerCleanX = null;
            boolean v;
            try {
                directBufferCleanerX = Class.forName("java.nio.DirectByteBuffer").getMethod("cleaner");
                directBufferCleanerX.setAccessible(true);
                directBufferCleanerCleanX = Class.forName("sun.misc.Cleaner").getMethod("clean");
                directBufferCleanerCleanX.setAccessible(true);
                v = true;
            } catch (Exception e) {
                v = false;
            }
            CLEAN_SUPPORTED = v;
            directBufferCleaner = directBufferCleanerX;
            directBufferCleanerClean = directBufferCleanerCleanX;
        }

        public static void clean(ByteBuffer buffer) {
            if (CLEAN_SUPPORTED && buffer.isDirect()) {
                try {
                    Object cleaner = directBufferCleaner.invoke(buffer);
                    directBufferCleanerClean.invoke(cleaner);
                } catch (Exception e) {
                    // silently ignore exception
                }
            }
        }
    }

    public static enum Type {
        SMALL,
        LARGE
    }

    /**
     * The size (in bytes) that is allocated for the provided type.
     */
    int sizeInBytes(Type type);

    /**
     * Allocate a byte buffer for the specific type.
     */
    ByteBuffer allocate(Type type) throws IOException;

    /**
     * Release the buffer.
     */
    void release(ByteBuffer buffer);

    /**
     * Close the allocator, releasing any cached buffers for example.
     */
    void close();
}

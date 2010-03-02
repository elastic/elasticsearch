/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.index.store.memory;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.SingleInstanceLockFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A memory based directory that uses {@link java.nio.ByteBuffer} in order to store the directory content.
 *
 * <p>The benefit of using {@link java.nio.ByteBuffer} is the fact that it can be stored in "native" memory
 * outside of the JVM heap, thus not incurring the GC overhead of large in memory index.
 *
 * <p>Each "file" is segmented into one or more byte buffers.
 *
 * <p>Since its good practice to cache byte buffers, it also provide a simple mechanism to define a cache
 * of byte buffers that are reused when possible.
 *
 * @author kimchy (shay.banon)
 */
public class ByteBufferDirectory extends Directory {

    private final Map<String, ByteBufferFile> files = new ConcurrentHashMap<String, ByteBufferFile>();

    private final Queue<ByteBuffer> cache;

    private final int bufferSizeInBytes;

    private final int cacheSizeInBytes;

    private final boolean disableCache;

    private final boolean direct;

    private boolean useCleanHack = true;

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


    /**
     * Constructs a new byte buffer directory.
     *
     * @param bufferSizeInBytes The size of a byte buffer
     * @param cacheSizeInBytes  The size of the cache, set to <code>0</code> to disable caching
     * @param direct            Should the byte buffers be stored outside the heap (<code>true</code) or in head (<code>false</code>)
     * @param warmCache         Should the cache be warmed
     */
    public ByteBufferDirectory(int bufferSizeInBytes, int cacheSizeInBytes, boolean direct, boolean warmCache) {
        disableCache = cacheSizeInBytes == 0;
        if (!disableCache && cacheSizeInBytes < bufferSizeInBytes) {
            throw new IllegalArgumentException("Cache size [" + cacheSizeInBytes + "] is smaller than buffer size [" + bufferSizeInBytes + "]");
        }
        this.bufferSizeInBytes = bufferSizeInBytes;
        int numberOfCacheEntries = cacheSizeInBytes / bufferSizeInBytes;
        this.cache = disableCache ? null : new ArrayBlockingQueue<ByteBuffer>(numberOfCacheEntries);
        this.cacheSizeInBytes = disableCache ? 0 : numberOfCacheEntries * bufferSizeInBytes;
        this.direct = direct;
        setLockFactory(new SingleInstanceLockFactory());
        if (!disableCache && warmCache) {
            for (int i = 0; i < numberOfCacheEntries; i++) {
                cache.add(createBuffer());
            }
        }
    }

    public void setUseClean(final boolean useCleanHack) {
        if (useCleanHack && !CLEAN_SUPPORTED)
            throw new IllegalArgumentException("Clean hack not supported on this platform!");
        this.useCleanHack = useCleanHack;
    }

    public boolean getUseClean() {
        return useCleanHack;
    }

    public int cacheSizeInBytes() {
        return this.cacheSizeInBytes;
    }

    public int bufferSizeInBytes() {
        return bufferSizeInBytes;
    }

    public boolean isDirect() {
        return direct;
    }

    public boolean isCacheEnabled() {
        return !disableCache;
    }

    @Override public String[] listAll() throws IOException {
        return files.keySet().toArray(new String[0]);
    }

    @Override public boolean fileExists(String name) throws IOException {
        return files.containsKey(name);
    }

    @Override public long fileModified(String name) throws IOException {
        ByteBufferFile file = files.get(name);
        if (file == null)
            throw new FileNotFoundException(name);
        return file.lastModified();
    }

    @Override public void touchFile(String name) throws IOException {
        ByteBufferFile file = files.get(name);
        if (file == null)
            throw new FileNotFoundException(name);

        long ts2, ts1 = System.currentTimeMillis();
        do {
            try {
                Thread.sleep(0, 1);
            } catch (InterruptedException ie) {
                // In 3.0 we will change this to throw
                // InterruptedException instead
                Thread.currentThread().interrupt();
                throw new RuntimeException(ie);
            }
            ts2 = System.currentTimeMillis();
        } while (ts1 == ts2);

        file.lastModified(ts2);
    }

    @Override public void deleteFile(String name) throws IOException {
        ByteBufferFile file = files.remove(name);
        if (file == null)
            throw new FileNotFoundException(name);
        file.clean();
    }

    @Override public long fileLength(String name) throws IOException {
        ByteBufferFile file = files.get(name);
        if (file == null)
            throw new FileNotFoundException(name);
        return file.length();
    }

    @Override public IndexOutput createOutput(String name) throws IOException {
        ByteBufferFile file = new ByteBufferFile(this);
        ByteBufferFile existing = files.put(name, file);
        if (existing != null) {
            existing.clean();
        }
        return new ByteBufferIndexOutput(this, file);
    }

    @Override public IndexInput openInput(String name) throws IOException {
        ByteBufferFile file = files.get(name);
        if (file == null)
            throw new FileNotFoundException(name);
        return new ByteBufferIndexInput(this, file);
    }

    @Override public void close() throws IOException {
        String[] files = listAll();
        for (String file : files) {
            deleteFile(file);
        }
        if (!disableCache) {
            ByteBuffer buffer = cache.poll();
            while (buffer != null) {
                closeBuffer(buffer);
                buffer = cache.poll();
            }
        }
    }

    void releaseBuffer(ByteBuffer byteBuffer) {
        if (disableCache) {
            closeBuffer(byteBuffer);
            return;
        }
        boolean success = cache.offer(byteBuffer);
        if (!success) {
            closeBuffer(byteBuffer);
        }
    }

    ByteBuffer acquireBuffer() {
        if (disableCache) {
            return createBuffer();
        }
        ByteBuffer byteBuffer = cache.poll();
        if (byteBuffer == null) {
            // everything is taken, return a new one
            return createBuffer();
        }
        byteBuffer.position(0);
        return byteBuffer;
    }

    private ByteBuffer createBuffer() {
        if (isDirect()) {
            return ByteBuffer.allocateDirect(bufferSizeInBytes());
        }
        return ByteBuffer.allocate(bufferSizeInBytes());
    }

    private void closeBuffer(ByteBuffer byteBuffer) {
        if (useCleanHack && isDirect()) {
            try {
                Object cleaner = directBufferCleaner.invoke(byteBuffer);
                directBufferCleanerClean.invoke(cleaner);
            } catch (Exception e) {
                e.printStackTrace();
                // silently ignore exception
            }
        }
    }
}

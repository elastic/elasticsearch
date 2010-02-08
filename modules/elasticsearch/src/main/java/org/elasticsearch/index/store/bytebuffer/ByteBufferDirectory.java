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

package org.elasticsearch.index.store.bytebuffer;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.SingleInstanceLockFactory;
import org.elasticsearch.util.SizeUnit;
import org.elasticsearch.util.SizeValue;
import org.elasticsearch.util.concurrent.highscalelib.NonBlockingHashMap;
import sun.nio.ch.DirectBuffer;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * @author kimchy (Shay Banon)
 */
public class ByteBufferDirectory extends Directory {

    private final Map<String, ByteBufferFile> files = new NonBlockingHashMap<String, ByteBufferFile>();

    private final Queue<ByteBuffer> cache;

    private final int bufferSizeInBytes;

    private final SizeValue bufferSize;

    private final SizeValue cacheSize;

    private final boolean disableCache;

    private final boolean direct;

    public ByteBufferDirectory() {
        this(new SizeValue(1, SizeUnit.KB), new SizeValue(20, SizeUnit.MB), false, false);
    }

    public ByteBufferDirectory(SizeValue bufferSize, SizeValue cacheSize, boolean direct, boolean warmCache) {
        disableCache = cacheSize.bytes() == 0;
        if (!disableCache && cacheSize.bytes() < bufferSize.bytes()) {
            throw new IllegalArgumentException("Cache size [" + cacheSize + "] is smaller than buffer size [" + bufferSize + "]");
        }
        this.bufferSize = bufferSize;
        this.bufferSizeInBytes = (int) bufferSize.bytes();
        int numberOfCacheEntries = (int) (cacheSize.bytes() / bufferSize.bytes());
        this.cache = disableCache ? null : new ArrayBlockingQueue<ByteBuffer>(numberOfCacheEntries);
        this.cacheSize = disableCache ? new SizeValue(0, SizeUnit.BYTES) : new SizeValue(numberOfCacheEntries * bufferSize.bytes(), SizeUnit.BYTES);
        this.direct = direct;
        setLockFactory(new SingleInstanceLockFactory());
        if (!disableCache && warmCache) {
            for (int i = 0; i < numberOfCacheEntries; i++) {
                cache.add(createBuffer());
            }
        }
    }

    public SizeValue bufferSize() {
        return this.bufferSize;
    }

    public SizeValue cacheSize() {
        return this.cacheSize;
    }

    int bufferSizeInBytes() {
        return bufferSizeInBytes;
    }

    public boolean isDirect() {
        return direct;
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

    ByteBuffer createBuffer() {
        if (isDirect()) {
            return ByteBuffer.allocateDirect(bufferSizeInBytes());
        }
        return ByteBuffer.allocate(bufferSizeInBytes());
    }

    void closeBuffer(ByteBuffer byteBuffer) {
        if (isDirect()) {
            ((DirectBuffer) byteBuffer).cleaner().clean();
        }
    }
}

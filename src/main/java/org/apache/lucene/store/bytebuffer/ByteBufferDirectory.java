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

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.SingleInstanceLockFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A memory based directory that uses {@link java.nio.ByteBuffer} in order to store the directory content.
 * <p/>
 * <p>The benefit of using {@link java.nio.ByteBuffer} is the fact that it can be stored in "native" memory
 * outside of the JVM heap, thus not incurring the GC overhead of large in memory index.
 * <p/>
 * <p>Each "file" is segmented into one or more byte buffers.
 * <p/>
 * <p>If constructed with {@link ByteBufferAllocator}, it allows to control the allocation and release of
 * byte buffer. For example, custom implementations can include caching of byte buffers.
 */
public class ByteBufferDirectory extends Directory {

    protected final Map<String, ByteBufferFile> files = new ConcurrentHashMap<String, ByteBufferFile>();

    private final ByteBufferAllocator allocator;

    private final boolean internalAllocator;

    final AtomicLong sizeInBytes = new AtomicLong();

    /**
     * Constructs a new directory using {@link PlainByteBufferAllocator}.
     */
    public ByteBufferDirectory() {
        this.allocator = new PlainByteBufferAllocator(false, 1024, 1024 * 10);
        this.internalAllocator = true;
        try {
            setLockFactory(new SingleInstanceLockFactory());
        } catch (IOException e) {
            // will not happen
        }
    }

    /**
     * Constructs a new byte buffer directory with a custom allocator.
     */
    public ByteBufferDirectory(ByteBufferAllocator allocator) {
        this.allocator = allocator;
        this.internalAllocator = false;
        try {
            setLockFactory(new SingleInstanceLockFactory());
        } catch (IOException e) {
            // will not happen
        }
    }

    /**
     * Returns the size in bytes of the directory, chunk by buffer size.
     */
    public long sizeInBytes() {
        return sizeInBytes.get();
    }

    public void sync(Collection<String> names) throws IOException {
        // nothing to do here
    }

    @Override
    public String[] listAll() throws IOException {
        return files.keySet().toArray(new String[0]);
    }

    @Override
    public boolean fileExists(String name) throws IOException {
        return files.containsKey(name);
    }

    @Override
    public long fileModified(String name) throws IOException {
        ByteBufferFile file = files.get(name);
        if (file == null)
            throw new FileNotFoundException(name);
        return file.getLastModified();
    }

    @Override
    public void touchFile(String name) throws IOException {
        ByteBufferFile file = files.get(name);
        if (file == null)
            throw new FileNotFoundException(name);

        long ts2, ts1 = System.currentTimeMillis();
        do {
            try {
                Thread.sleep(0, 1);
            } catch (java.lang.InterruptedException ie) {
                // In 3.0 we will change this to throw
                // InterruptedException instead
                Thread.currentThread().interrupt();
                throw new RuntimeException(ie);
            }
            ts2 = System.currentTimeMillis();
        } while (ts1 == ts2);

        file.setLastModified(ts2);
    }

    @Override
    public void deleteFile(String name) throws IOException {
        ByteBufferFile file = files.remove(name);
        if (file == null)
            throw new FileNotFoundException(name);
        sizeInBytes.addAndGet(-file.sizeInBytes());
        file.delete();
    }

    @Override
    public long fileLength(String name) throws IOException {
        ByteBufferFile file = files.get(name);
        if (file == null)
            throw new FileNotFoundException(name);
        return file.getLength();
    }

    @Override
    public IndexOutput createOutput(String name) throws IOException {
        ByteBufferAllocator.Type allocatorType = ByteBufferAllocator.Type.LARGE;
        if (name.contains("segments") || name.endsWith(".del")) {
            allocatorType = ByteBufferAllocator.Type.SMALL;
        }
        ByteBufferFileOutput file = new ByteBufferFileOutput(this, allocator.sizeInBytes(allocatorType));
        ByteBufferFile existing = files.put(name, file);
        if (existing != null) {
            sizeInBytes.addAndGet(-existing.sizeInBytes());
            existing.delete();
        }
        return new ByteBufferIndexOutput(this, name, allocator, allocatorType, file);
    }

    void closeOutput(String name, ByteBufferFileOutput file) {
        // we replace the output file with a read only file, with no sync
        files.put(name, new ByteBufferFile(file));
    }

    @Override
    public IndexInput openInput(String name) throws IOException {
        ByteBufferFile file = files.get(name);
        if (file == null)
            throw new FileNotFoundException(name);
        return new ByteBufferIndexInput(name, file);
    }

    @Override
    public void close() throws IOException {
        String[] files = listAll();
        for (String file : files) {
            deleteFile(file);
        }
        if (internalAllocator) {
            allocator.close();
        }
    }

    void releaseBuffer(ByteBuffer byteBuffer) {
        allocator.release(byteBuffer);
    }
}

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


import org.apache.lucene.store.IndexOutput;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 */
public class ByteBufferIndexOutput extends IndexOutput {

    private final static ByteBuffer EMPTY_BUFFER = ByteBuffer.allocate(0).asReadOnlyBuffer();

    private final ByteBufferDirectory dir;
    private final String name;
    private final ByteBufferAllocator allocator;
    private final ByteBufferAllocator.Type allocatorType;
    private final int BUFFER_SIZE;
    private final ByteBufferFileOutput file;

    private ByteBuffer currentBuffer;
    private int currentBufferIndex;

    private long bufferStart;

    public ByteBufferIndexOutput(ByteBufferDirectory dir, String name, ByteBufferAllocator allocator, ByteBufferAllocator.Type allocatorType, ByteBufferFileOutput file) throws IOException {
        this.dir = dir;
        this.name = name;
        this.allocator = allocator;
        this.allocatorType = allocatorType;
        this.BUFFER_SIZE = file.bufferSize;
        this.file = file;

        currentBufferIndex = -1;
        currentBuffer = EMPTY_BUFFER;
    }

    @Override
    public void close() throws IOException {
        flush();
        dir.closeOutput(name, file);
    }

    @Override
    public void seek(long pos) throws IOException {
        // set the file length in case we seek back
        // and flush() has not been called yet
        setFileLength();
        if (pos < bufferStart || pos >= bufferStart + BUFFER_SIZE) {
            currentBufferIndex = (int) (pos / BUFFER_SIZE);
            switchCurrentBuffer();
        }
        currentBuffer.position((int) (pos % BUFFER_SIZE));
    }

    @Override
    public long length() {
        return file.getLength();
    }

    @Override
    public void writeByte(byte b) throws IOException {
        if (!currentBuffer.hasRemaining()) {
            currentBufferIndex++;
            switchCurrentBuffer();
        }
        currentBuffer.put(b);
    }

    @Override
    public void writeBytes(byte[] b, int offset, int len) throws IOException {
        while (len > 0) {
            if (!currentBuffer.hasRemaining()) {
                currentBufferIndex++;
                switchCurrentBuffer();
            }

            int remainInBuffer = currentBuffer.remaining();
            int bytesToCopy = len < remainInBuffer ? len : remainInBuffer;
            currentBuffer.put(b, offset, bytesToCopy);
            offset += bytesToCopy;
            len -= bytesToCopy;
        }
    }

    private void switchCurrentBuffer() throws IOException {
        if (currentBufferIndex == file.numBuffers()) {
            currentBuffer = allocator.allocate(allocatorType);
            file.addBuffer(currentBuffer);
        } else {
            currentBuffer = file.getBuffer(currentBufferIndex);
        }
        currentBuffer.position(0);
        bufferStart = (long) BUFFER_SIZE * (long) currentBufferIndex;
    }

    private void setFileLength() {
        long pointer = bufferStart + currentBuffer.position();
        if (pointer > file.getLength()) {
            file.setLength(pointer);
        }
    }

    @Override
    public void flush() throws IOException {
        file.setLastModified(System.currentTimeMillis());
        setFileLength();
    }

    @Override
    public long getFilePointer() {
        return currentBufferIndex < 0 ? 0 : bufferStart + currentBuffer.position();
    }
}

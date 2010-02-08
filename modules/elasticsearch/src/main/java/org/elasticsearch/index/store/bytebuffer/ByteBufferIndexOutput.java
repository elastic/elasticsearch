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

import org.apache.lucene.store.IndexOutput;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;

/**
 * @author kimchy (Shay Banon)
 */
public class ByteBufferIndexOutput extends IndexOutput {

    private final ByteBufferDirectory dir;
    private final ByteBufferFile file;

    private ByteBuffer currentBuffer;
    private int currentBufferIndex;

    private long bufferStart;
    private int bufferLength;

    private ArrayList<ByteBuffer> buffers = new ArrayList<ByteBuffer>();

    public ByteBufferIndexOutput(ByteBufferDirectory dir, ByteBufferFile file) throws IOException {
        this.dir = dir;
        this.file = file;
        switchCurrentBuffer();
    }

    @Override public void writeByte(byte b) throws IOException {
        if (!currentBuffer.hasRemaining()) {
            currentBufferIndex++;
            switchCurrentBuffer();
        }
        currentBuffer.put(b);
    }

    @Override public void writeBytes(byte[] b, int offset, int len) throws IOException {
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

    @Override public void flush() throws IOException {
        file.lastModified(System.currentTimeMillis());
        setFileLength();
    }

    @Override public void close() throws IOException {
        flush();
        file.buffers(buffers.toArray(new ByteBuffer[buffers.size()]));
    }

    @Override public long getFilePointer() {
        return currentBufferIndex < 0 ? 0 : bufferStart + currentBuffer.position();
    }

    @Override public void seek(long pos) throws IOException {
        // set the file length in case we seek back
        // and flush() has not been called yet
        setFileLength();
        if (pos < bufferStart || pos >= bufferStart + bufferLength) {
            currentBufferIndex = (int) (pos / dir.bufferSizeInBytes());
            switchCurrentBuffer();
        }
        currentBuffer.position((int) (pos % dir.bufferSizeInBytes()));
    }

    @Override public long length() throws IOException {
        return file.length();
    }

    private void switchCurrentBuffer() throws IOException {
        if (currentBufferIndex == buffers.size()) {
            currentBuffer = dir.acquireBuffer();
            buffers.add(currentBuffer);
        } else {
            currentBuffer = buffers.get(currentBufferIndex);
        }
        currentBuffer.position(0);
        bufferStart = (long) dir.bufferSizeInBytes() * (long) currentBufferIndex;
        bufferLength = currentBuffer.capacity();
    }

    private void setFileLength() {
        long pointer = bufferStart + currentBuffer.position();
        if (pointer > file.length()) {
            file.length(pointer);
        }
    }
}

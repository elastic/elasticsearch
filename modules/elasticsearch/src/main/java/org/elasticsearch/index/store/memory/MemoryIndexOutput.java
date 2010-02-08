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

import org.apache.lucene.store.IndexOutput;

import java.io.IOException;
import java.util.ArrayList;

/**
 * @author kimchy (Shay Banon)
 */
public class MemoryIndexOutput extends IndexOutput {

    private final MemoryDirectory dir;
    private final MemoryFile file;

    private ArrayList<byte[]> buffers = new ArrayList<byte[]>();

    private byte[] currentBuffer;
    private int currentBufferIndex;

    private int bufferPosition;
    private long bufferStart;
    private int bufferLength;

    public MemoryIndexOutput(MemoryDirectory dir, MemoryFile file) {
        this.dir = dir;
        this.file = file;

        // make sure that we switch to the
        // first needed buffer lazily
        currentBufferIndex = -1;
        currentBuffer = null;
    }

    @Override public void writeByte(byte b) throws IOException {
        if (bufferPosition == bufferLength) {
            currentBufferIndex++;
            switchCurrentBuffer();
        }
        currentBuffer[bufferPosition++] = b;
    }

    @Override public void writeBytes(byte[] b, int offset, int len) throws IOException {
        while (len > 0) {
            if (bufferPosition == bufferLength) {
                currentBufferIndex++;
                switchCurrentBuffer();
            }

            int remainInBuffer = currentBuffer.length - bufferPosition;
            int bytesToCopy = len < remainInBuffer ? len : remainInBuffer;
            System.arraycopy(b, offset, currentBuffer, bufferPosition, bytesToCopy);
            offset += bytesToCopy;
            len -= bytesToCopy;
            bufferPosition += bytesToCopy;
        }
    }

    @Override public void flush() throws IOException {
        file.lastModified(System.currentTimeMillis());
        setFileLength();
    }

    @Override public void close() throws IOException {
        flush();
        file.buffers(buffers.toArray(new byte[buffers.size()][]));
    }

    @Override public long getFilePointer() {
        return currentBufferIndex < 0 ? 0 : bufferStart + bufferPosition;
    }

    @Override public void seek(long pos) throws IOException {
        // set the file length in case we seek back
        // and flush() has not been called yet
        setFileLength();
        if (pos < bufferStart || pos >= bufferStart + bufferLength) {
            currentBufferIndex = (int) (pos / dir.bufferSizeInBytes());
            switchCurrentBuffer();
        }

        bufferPosition = (int) (pos % dir.bufferSizeInBytes());
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
        bufferPosition = 0;
        bufferStart = (long) dir.bufferSizeInBytes() * (long) currentBufferIndex;
        bufferLength = currentBuffer.length;
    }

    private void setFileLength() {
        long pointer = bufferStart + bufferPosition;
        if (pointer > file.length()) {
            file.length(pointer);
        }
    }
}

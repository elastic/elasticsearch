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

import org.apache.lucene.store.IndexInput;

import java.io.IOException;

/**
 * @author kimchy (Shay Banon)
 */
public class MemoryIndexInput extends IndexInput {

    private final int bufferSize;
    private final MemoryFile file;

    private long length;

    private byte[] currentBuffer;
    private int currentBufferIndex;

    private int bufferPosition;
    private long bufferStart;
    private int bufferLength;

    public MemoryIndexInput(MemoryDirectory dir, MemoryFile file) throws IOException {
        this.bufferSize = dir.bufferSizeInBytes();
        this.file = file;

        length = file.length();
        if (length / dir.bufferSizeInBytes() >= Integer.MAX_VALUE) {
            throw new IOException("Too large RAMFile! " + length);
        }

        // make sure that we switch to the
        // first needed buffer lazily
        currentBufferIndex = -1;
        currentBuffer = null;
    }

    @Override public byte readByte() throws IOException {
        if (bufferPosition >= bufferLength) {
            currentBufferIndex++;
            switchCurrentBuffer(true);
        }
        return currentBuffer[bufferPosition++];
    }

    @Override public void readBytes(byte[] b, int offset, int len) throws IOException {
        while (len > 0) {
            if (bufferPosition >= bufferLength) {
                currentBufferIndex++;
                switchCurrentBuffer(true);
            }

            int remainInBuffer = bufferLength - bufferPosition;
            int bytesToCopy = len < remainInBuffer ? len : remainInBuffer;
            System.arraycopy(currentBuffer, bufferPosition, b, offset, bytesToCopy);
            offset += bytesToCopy;
            len -= bytesToCopy;
            bufferPosition += bytesToCopy;
        }
    }

    @Override public void close() throws IOException {
    }

    @Override public long getFilePointer() {
        return currentBufferIndex < 0 ? 0 : bufferStart + bufferPosition;
    }

    @Override public void seek(long pos) throws IOException {
        if (currentBuffer == null || pos < bufferStart || pos >= bufferStart + bufferSize) {
            currentBufferIndex = (int) (pos / bufferSize);
            switchCurrentBuffer(false);
        }
        bufferPosition = (int) (pos % bufferSize);
    }

    @Override public long length() {
        return length;
    }

    private void switchCurrentBuffer(boolean enforceEOF) throws IOException {
        if (currentBufferIndex >= file.numberOfBuffers()) {
            // end of file reached, no more buffers left
            if (enforceEOF)
                throw new IOException("Read past EOF");
            else {
                // Force EOF if a read takes place at this position
                currentBufferIndex--;
                bufferPosition = bufferSize;
            }
        } else {
            currentBuffer = file.buffer(currentBufferIndex);
            bufferPosition = 0;
            bufferStart = (long) bufferSize * (long) currentBufferIndex;
            long buflen = length - bufferStart;
            bufferLength = buflen > bufferSize ? bufferSize : (int) buflen;
        }
    }
}

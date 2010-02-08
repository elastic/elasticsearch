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

import org.apache.lucene.store.IndexInput;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * @author kimchy (Shay Banon)
 */
public class ByteBufferIndexInput extends IndexInput {

    private final ByteBufferFile file;
    private final int bufferSize;
    private final long length;

    private ByteBuffer currentBuffer;
    private int currentBufferIndex;

    private long bufferStart;


    public ByteBufferIndexInput(ByteBufferDirectory dir, ByteBufferFile file) throws IOException {
        this.file = file;
        this.bufferSize = dir.bufferSizeInBytes();
        this.length = file.length();
        switchCurrentBuffer(true);
    }

    @Override public byte readByte() throws IOException {
        if (!currentBuffer.hasRemaining()) {
            currentBufferIndex++;
            switchCurrentBuffer(true);
        }
        return currentBuffer.get();
    }

    @Override public void readBytes(byte[] b, int offset, int len) throws IOException {
        while (len > 0) {
            if (!currentBuffer.hasRemaining()) {
                currentBufferIndex++;
                switchCurrentBuffer(true);
            }

            int remainInBuffer = currentBuffer.remaining();
            int bytesToCopy = len < remainInBuffer ? len : remainInBuffer;
            currentBuffer.get(b, offset, bytesToCopy);
            offset += bytesToCopy;
            len -= bytesToCopy;
        }
    }

    @Override public void close() throws IOException {
    }

    @Override public long getFilePointer() {
        return currentBufferIndex < 0 ? 0 : bufferStart + currentBuffer.position();
    }

    @Override public void seek(long pos) throws IOException {
        if (currentBuffer == null || pos < bufferStart || pos >= bufferStart + bufferSize) {
            currentBufferIndex = (int) (pos / bufferSize);
            switchCurrentBuffer(false);
        }
        currentBuffer.position((int) (pos % bufferSize));
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
                currentBuffer.position(bufferSize);
            }
        } else {
            // we must duplicate (and make it read only while we are at it) since we need position and such to be independant
            currentBuffer = file.buffer(currentBufferIndex).asReadOnlyBuffer();
            currentBuffer.position(0);
            bufferStart = (long) bufferSize * (long) currentBufferIndex;
        }
    }

    @Override public Object clone() {
        ByteBufferIndexInput cloned = (ByteBufferIndexInput) super.clone();
        cloned.currentBuffer = currentBuffer.asReadOnlyBuffer();
        return cloned;
    }
}

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

import org.apache.lucene.store.IndexInput;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * @author kimchy (shay.banon)
 */
public class ByteBufferIndexInput extends IndexInput {

    private final static ByteBuffer EMPTY_BUFFER = ByteBuffer.allocate(0).asReadOnlyBuffer();

    private final ByteBufferFile file;
    private final long length;

    private ByteBuffer currentBuffer;
    private int currentBufferIndex;

    private long bufferStart;
    private final int BUFFER_SIZE;

    public ByteBufferIndexInput(ByteBufferFile file) throws IOException {
        this.file = file;
        this.length = file.getLength();
        this.BUFFER_SIZE = file.bufferSize;

        // make sure that we switch to the
        // first needed buffer lazily
        currentBufferIndex = -1;
        currentBuffer = EMPTY_BUFFER;
    }

    @Override
    public void close() {
        // nothing to do here
    }

    @Override
    public long length() {
        return length;
    }

//    @Override
//    public short readShort() throws IOException {
//        try {
//            return currentBuffer.getShort();
//        } catch (BufferUnderflowException e) {
//            return super.readShort();
//        }
//    }
//
//    @Override
//    public int readInt() throws IOException {
//        try {
//            return currentBuffer.getInt();
//        } catch (BufferUnderflowException e) {
//            return super.readInt();
//        }
//    }
//
//    @Override
//    public long readLong() throws IOException {
//        try {
//            return currentBuffer.getLong();
//        } catch (BufferUnderflowException e) {
//            return super.readLong();
//        }
//    }

    @Override
    public byte readByte() throws IOException {
        if (!currentBuffer.hasRemaining()) {
            currentBufferIndex++;
            switchCurrentBuffer(true);
        }
        return currentBuffer.get();
    }

    @Override
    public void readBytes(byte[] b, int offset, int len) throws IOException {
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

    @Override
    public long getFilePointer() {
        return currentBufferIndex < 0 ? 0 : bufferStart + currentBuffer.position();
    }

    @Override
    public void seek(long pos) throws IOException {
        if (currentBuffer == EMPTY_BUFFER || pos < bufferStart || pos >= bufferStart + BUFFER_SIZE) {
            currentBufferIndex = (int) (pos / BUFFER_SIZE);
            if (currentBufferIndex >= file.numBuffers()) {
                // if we are past EOF, don't throw one here, instead, move it to the last position in the last buffer
                currentBufferIndex = file.numBuffers() - 1;
                currentBuffer = currentBufferIndex == -1 ? EMPTY_BUFFER : file.getBuffer(currentBufferIndex);
                currentBuffer.position(currentBuffer.limit());
                return;
            } else {
                switchCurrentBuffer(false);
            }
        }
        try {
            currentBuffer.position((int) (pos % BUFFER_SIZE));
        } catch (IllegalArgumentException e) {
            currentBuffer.position(currentBuffer.limit());
        }
    }

    private void switchCurrentBuffer(boolean enforceEOF) throws IOException {
        if (currentBufferIndex >= file.numBuffers()) {
            if (enforceEOF) {
                throw new IOException("Read past EOF");
            }
        } else {
            ByteBuffer buffer = file.getBuffer(currentBufferIndex);
            // we must duplicate (and make it read only while we are at it) since we need position and such to be independent
            currentBuffer = buffer.asReadOnlyBuffer();
            currentBuffer.position(0);
            bufferStart = (long) BUFFER_SIZE * (long) currentBufferIndex;
            // if we are at the tip, limit the current buffer to only whats available to read
            long buflen = length - bufferStart;
            if (buflen < BUFFER_SIZE) {
                currentBuffer.limit((int) buflen);
                if (enforceEOF && buflen == 0) {
                    throw new IOException("Read past EOF");
                }
            }
        }
    }

    @Override
    public Object clone() {
        ByteBufferIndexInput cloned = (ByteBufferIndexInput) super.clone();
        if (currentBuffer != EMPTY_BUFFER) {
            cloned.currentBuffer = currentBuffer.asReadOnlyBuffer();
            cloned.currentBuffer.position(currentBuffer.position());
        }
        return cloned;
    }
}

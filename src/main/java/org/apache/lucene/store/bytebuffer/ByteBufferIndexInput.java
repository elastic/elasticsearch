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

import java.io.EOFException;
import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;

/**
 */
public class ByteBufferIndexInput extends IndexInput {

    private final static ByteBuffer EMPTY_BUFFER = ByteBuffer.allocate(0).asReadOnlyBuffer();

    private final ByteBufferFile file;
    private final long length;

    private ByteBuffer currentBuffer;
    private int currentBufferIndex;

    private long bufferStart;
    private final int BUFFER_SIZE;

    private volatile boolean closed = false;

    public ByteBufferIndexInput(String name, ByteBufferFile file) throws IOException {
        super("BBIndexInput(name=" + name + ")");
        this.file = file;
        this.file.incRef();
        this.length = file.getLength();
        this.BUFFER_SIZE = file.bufferSize;

        // make sure that we switch to the
        // first needed buffer lazily
        currentBufferIndex = -1;
        currentBuffer = EMPTY_BUFFER;
    }

    @Override
    public void close() {
        // we protected from double closing the index input since
        // some tests do that...
        if (closed) {
            return;
        }
        closed = true;
        file.decRef();
    }

    @Override
    public long length() {
        return length;
    }

    @Override
    public short readShort() throws IOException {
        try {
            currentBuffer.mark();
            return currentBuffer.getShort();
        } catch (BufferUnderflowException e) {
            currentBuffer.reset();
            return super.readShort();
        }
    }

    @Override
    public int readInt() throws IOException {
        try {
            currentBuffer.mark();
            return currentBuffer.getInt();
        } catch (BufferUnderflowException e) {
            currentBuffer.reset();
            return super.readInt();
        }
    }

    @Override
    public long readLong() throws IOException {
        try {
            currentBuffer.mark();
            return currentBuffer.getLong();
        } catch (BufferUnderflowException e) {
            currentBuffer.reset();
            return super.readLong();
        }
    }

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
            switchCurrentBuffer(false);
        }
        try {
            currentBuffer.position((int) (pos % BUFFER_SIZE));
            // Grrr, need to wrap in IllegalArgumentException since tests (if not other places)
            // expect an IOException...
        } catch (IllegalArgumentException e) {
            IOException ioException = new IOException("seeking past position");
            ioException.initCause(e);
            throw ioException;
        }
    }

    private void switchCurrentBuffer(boolean enforceEOF) throws IOException {
        if (currentBufferIndex >= file.numBuffers()) {
            // end of file reached, no more buffers left
            if (enforceEOF) {
                throw new EOFException("Read past EOF (resource: " + this + ")");
            } else {
                // Force EOF if a read takes place at this position
                currentBufferIndex--;
                currentBuffer.position(currentBuffer.limit());
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
            }

            // we need to enforce EOF here as well...
            if (!currentBuffer.hasRemaining()) {
                if (enforceEOF) {
                    throw new EOFException("Read past EOF (resource: " + this + ")");
                } else {
                    // Force EOF if a read takes place at this position
                    currentBufferIndex--;
                    currentBuffer.position(currentBuffer.limit());
                }
            }
        }
    }

    @Override
    public IndexInput clone() {
        ByteBufferIndexInput cloned = (ByteBufferIndexInput) super.clone();
        cloned.file.incRef(); // inc ref on cloned one
        if (currentBuffer != EMPTY_BUFFER) {
            cloned.currentBuffer = currentBuffer.asReadOnlyBuffer();
            cloned.currentBuffer.position(currentBuffer.position());
        }
        return cloned;
    }
}

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

package org.apache.lucene.store;

import java.io.IOException;
import java.util.zip.Checksum;

/**
 */
public class BufferedChecksumIndexOutput extends BufferedIndexOutput {

    private final IndexOutput delegate;
    private final BufferedIndexOutput bufferedDelegate;
    private final Checksum digest;

    public BufferedChecksumIndexOutput(IndexOutput delegate, Checksum digest) {
        super(delegate instanceof BufferedIndexOutput ? ((BufferedIndexOutput) delegate).getBufferSize() : BufferedIndexOutput.DEFAULT_BUFFER_SIZE);
        if (delegate instanceof BufferedIndexOutput) {
            bufferedDelegate = (BufferedIndexOutput) delegate;
            this.delegate = delegate;
        } else {
            this.delegate = delegate;
            bufferedDelegate = null;
        }
        this.digest = digest;
    }

    public Checksum digest() {
        return digest;
    }

    public IndexOutput underlying() {
        return this.delegate;
    }

    // don't override it, base class method simple reads from input and writes to this output
//        @Override public void copyBytes(IndexInput input, long numBytes) throws IOException {
//            delegate.copyBytes(input, numBytes);
//        }

    @Override
    public void close() throws IOException {
        try {
            super.close();
        } finally {
            delegate.close();
        }
        
    }

    @Override
    protected void flushBuffer(byte[] b, int offset, int len) throws IOException {
        if (bufferedDelegate != null) {
            bufferedDelegate.flushBuffer(b, offset, len);
        } else {
            delegate.writeBytes(b, offset, len);
        }
        digest.update(b, offset, len);
    }

    // don't override it, base class method simple reads from input and writes to this output
//        @Override public void copyBytes(IndexInput input, long numBytes) throws IOException {
//            delegate.copyBytes(input, numBytes);
//        }

    @Override
    public void flush() throws IOException {
        try {
            super.flush();
        } finally {
            delegate.flush();
        }
    }

    @Override
    public void seek(long pos) throws IOException {
        // seek might be called on files, which means that the checksum is not file checksum
        // but a checksum of the bytes written to this stream, which is the same for each
        // type of file in lucene
        super.seek(pos);
        delegate.seek(pos);
    }

    @Override
    public long length() throws IOException {
        return delegate.length();
    }

    @Override
    public void setLength(long length) throws IOException {
        delegate.setLength(length);
    }

    @Override
    public String toString() {
        return delegate.toString();
    }
}

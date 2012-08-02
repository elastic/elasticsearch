/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
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

package org.elasticsearch.common.lucene.store;

import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.OpenBufferedIndexOutput;

import java.io.IOException;
import java.util.zip.Checksum;

/**
 */
public class BufferedChecksumIndexOutput extends OpenBufferedIndexOutput {

    private final IndexOutput out;

    private final Checksum digest;

    public BufferedChecksumIndexOutput(IndexOutput out, Checksum digest) {
        // we add 8 to be bigger than the default BufferIndexOutput buffer size so any flush will go directly
        // to the output without being copied over to the delegate buffer
        super(OpenBufferedIndexOutput.DEFAULT_BUFFER_SIZE + 64);
        this.out = out;
        this.digest = digest;
    }

    public Checksum digest() {
        return digest;
    }

    public IndexOutput underlying() {
        return this.out;
    }

    // don't override it, base class method simple reads from input and writes to this output
//        @Override public void copyBytes(IndexInput input, long numBytes) throws IOException {
//            delegate.copyBytes(input, numBytes);
//        }

    @Override
    public void close() throws IOException {
        super.close();
        out.close();
    }

    @Override
    protected void flushBuffer(byte[] b, int offset, int len) throws IOException {
        out.writeBytes(b, offset, len);
        digest.update(b, offset, len);
    }

    // don't override it, base class method simple reads from input and writes to this output
//        @Override public void copyBytes(IndexInput input, long numBytes) throws IOException {
//            delegate.copyBytes(input, numBytes);
//        }

    @Override
    public void flush() throws IOException {
        super.flush();
        out.flush();
    }

    @Override
    public void seek(long pos) throws IOException {
        // seek might be called on files, which means that the checksum is not file checksum
        // but a checksum of the bytes written to this stream, which is the same for each
        // type of file in lucene
        super.seek(pos);
        out.seek(pos);
    }

    @Override
    public long length() throws IOException {
        return out.length();
    }

    @Override
    public void setLength(long length) throws IOException {
        out.setLength(length);
    }

    @Override
    public String toString() {
        return out.toString();
    }
}

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

import java.io.IOException;
import java.util.zip.Checksum;

/**
 */
public class ChecksumIndexOutput extends IndexOutput {

    private final IndexOutput out;

    private final Checksum digest;

    public ChecksumIndexOutput(IndexOutput out, Checksum digest) {
        this.out = out;
        this.digest = digest;
    }

    public Checksum digest() {
        return digest;
    }

    @Override
    public void writeByte(byte b) throws IOException {
        out.writeByte(b);
        digest.update(b);
    }

    @Override
    public void setLength(long length) throws IOException {
        out.setLength(length);
    }

    // don't override copyBytes, since we need to read it and compute it
//    @Override
//    public void copyBytes(DataInput input, long numBytes) throws IOException {
//        super.copyBytes(input, numBytes);
//    }


    @Override
    public String toString() {
        return out.toString();
    }

    @Override
    public void writeBytes(byte[] b, int offset, int length) throws IOException {
        out.writeBytes(b, offset, length);
        digest.update(b, offset, length);
    }

    @Override
    public void flush() throws IOException {
        out.flush();
    }

    @Override
    public void close() throws IOException {
        out.close();
    }

    @Override
    public long getFilePointer() {
        return out.getFilePointer();
    }

    @Override
    public void seek(long pos) throws IOException {
        out.seek(pos);
    }

    @Override
    public long length() throws IOException {
        return out.length();
    }
}

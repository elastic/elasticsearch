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

/**
 *
 */
public class NullIndexOutput extends IndexOutput {

    private long length = 0;

    private long position = 0;

    @Override
    public void writeByte(byte b) throws IOException {
        position++;
        if (position > length) {
            length = position;
        }
    }

    @Override
    public void writeBytes(byte[] b, int offset, int length) throws IOException {
        position += length;
        if (position > this.length) {
            this.length = position;
        }
    }

    @Override
    public void flush() throws IOException {
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public long getFilePointer() {
        return position;
    }

    @Override
    public void seek(long pos) throws IOException {
        position = pos;
    }

    @Override
    public long length() throws IOException {
        return length;
    }
}

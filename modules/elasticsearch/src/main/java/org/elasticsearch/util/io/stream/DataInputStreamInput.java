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

package org.elasticsearch.util.io.stream;

import java.io.Closeable;
import java.io.DataInput;
import java.io.IOException;

/**
 * @author kimchy (shay.banon)
 */
public class DataInputStreamInput extends StreamInput {

    private final DataInput in;

    public DataInputStreamInput(DataInput in) {
        this.in = in;
    }

    @Override public int read() throws IOException {
        return in.readByte() & 0xFF;
    }

    @Override public byte readByte() throws IOException {
        return in.readByte();
    }

    @Override public void readBytes(byte[] b, int offset, int len) throws IOException {
        in.readFully(b, offset, len);
    }

    @Override public void reset() throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override public void close() throws IOException {
        if (in instanceof Closeable) {
            ((Closeable) in).close();
        }
    }
}
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

package org.elasticsearch.common.io.stream;

import java.io.Closeable;
import java.io.DataOutput;
import java.io.IOException;

public class DataOutputStreamOutput extends StreamOutput {

    private final DataOutput out;

    public DataOutputStreamOutput(DataOutput out) {
        this.out = out;
    }

    @Override
    public void writeByte(byte b) throws IOException {
        out.writeByte(b);
    }

    @Override
    public void writeBytes(byte[] b, int offset, int length) throws IOException {
        out.write(b, offset, length);
    }

    @Override
    public void flush() throws IOException {
        // nothing to do there...
    }

    @Override
    public void reset() throws IOException {
        // nothing to do there...
    }

    @Override
    public void close() throws IOException {
        if (out instanceof Closeable) {
            ((Closeable) out).close();
        }
    }
}
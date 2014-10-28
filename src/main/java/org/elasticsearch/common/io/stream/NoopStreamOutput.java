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

import java.io.IOException;

/**
 * A non-threadsafe StreamOutput that doesn't actually write the bytes to any
 * stream, it only keeps track of how many bytes have been written
 */
public final class NoopStreamOutput extends StreamOutput {

    private int count = 0;

    /** Retrieve the number of bytes that have been written */
    public int getCount() {
        return count;
    }

    @Override
    public void writeByte(byte b) throws IOException {
        count++;
    }

    @Override
    public void writeBytes(byte[] b, int offset, int length) throws IOException {
        count += length;
    }

    @Override
    public void flush() throws IOException {
        // no-op
    }

    @Override
    public void close() throws IOException {
        // nothing to close
    }

    @Override
    public void reset() throws IOException {
        count = 0;
    }
}

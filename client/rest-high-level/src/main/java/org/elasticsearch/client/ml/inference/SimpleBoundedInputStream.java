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
package org.elasticsearch.client.ml.inference;


import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;

/**
 * This is a pared down bounded input stream.
 * Only read is specifically enforced.
 */
final class SimpleBoundedInputStream extends InputStream {

    private final InputStream in;
    private final long maxBytes;
    private long numBytes;

    SimpleBoundedInputStream(InputStream inputStream, long maxBytes) {
        this.in = Objects.requireNonNull(inputStream, "inputStream");
        if (maxBytes < 0) {
            throw new IllegalArgumentException("[maxBytes] must be greater than or equal to 0");
        }
        this.maxBytes = maxBytes;
    }


    /**
     * A simple wrapper around the injected input stream that restricts the total number of bytes able to be read.
     * @return The byte read. -1 on internal stream completion or when maxBytes is exceeded.
     * @throws IOException on failure
     */
    @Override
    public int read() throws IOException {
        // We have reached the maximum, signal stream completion.
        if (numBytes >= maxBytes) {
            return -1;
        }
        numBytes++;
        return in.read();
    }

    /**
     * Delegates `close` to the wrapped InputStream
     * @throws IOException on failure
     */
    @Override
    public void close() throws IOException {
        in.close();
    }
}

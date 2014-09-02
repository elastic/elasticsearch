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

package org.elasticsearch.index.translog;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.Closeable;
import java.io.IOException;
import java.nio.channels.FileChannel;

/**
 * A translog stream that will read and write operations in the
 * version-specific format
 */
public interface TranslogStream extends Closeable {

    /**
     * Read the next operation from the translog file, the stream <b>must</b>
     * have been created through {@link TranslogStreams#translogStreamFor(java.io.File)}
     */
    public Translog.Operation read() throws IOException;

    /**
     * Read the next translog operation from the input stream
     */
    public Translog.Operation read(StreamInput in) throws IOException;

    /**
     * Read a translog operation from the given byte array, returning the
     * {@link Translog.Source} object from the Operation
     */
    public Translog.Source readSource(byte[] data) throws IOException;

    /**
     * Write the given translog operation to the output stream
     */
    public void write(StreamOutput out, Translog.Operation op) throws IOException;

    /**
     * Optionally write a header identifying the translog version to the
     * file channel
     */
    public int writeHeader(FileChannel channel) throws IOException;

    /**
     * Close the stream opened with {@link TranslogStreams#translogStreamFor(java.io.File)}
     */
    public void close() throws IOException;
}

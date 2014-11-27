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

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;

/**
 * A translog stream that will read and write operations in the
 * version-specific format
 */
public interface TranslogStream {

    /**
     * Read the next translog operation from the input stream
     */
    public Translog.Operation read(StreamInput in) throws IOException;

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
     * Seek past the header, if any header is present
     */
    public StreamInput openInput(Path translogFile) throws IOException;

}

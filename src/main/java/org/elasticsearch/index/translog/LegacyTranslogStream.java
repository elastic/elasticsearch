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

import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Version 0 of the translog format, there is no header in this file
 */
public class LegacyTranslogStream implements TranslogStream {

    LegacyTranslogStream() {
    }

    @Override
    public Translog.Operation read(StreamInput in) throws IOException {
        Translog.Operation.Type type = Translog.Operation.Type.fromId(in.readByte());
        Translog.Operation operation = TranslogStreams.newOperationFromType(type);
        operation.readFrom(in);
        return operation;
    }

    @Override
    public void write(StreamOutput out, Translog.Operation op) throws IOException {
        out.writeByte(op.opType().id());
        op.writeTo(out);
    }

    @Override
    public int writeHeader(FileChannel channel) {
        // nothing, there is no header for version 0 translog files
        return 0;
    }

    @Override
    public StreamInput openInput(Path translogFile) throws IOException {
        // nothing to do, legacy translogs have no header
        return new InputStreamStreamInput(Files.newInputStream(translogFile));
    }

}

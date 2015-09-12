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

import java.io.IOException;

/**
 * Version 0 of the translog format, there is no header in this file
 */
@Deprecated
public final class LegacyTranslogReader extends LegacyTranslogReaderBase {

    /**
     * Create a snapshot of translog file channel. The length parameter should be consistent with totalOperations and point
     * at the end of the last operation in this snapshot.
     *
     * @param generation
     * @param channelReference
     */
    LegacyTranslogReader(long generation, ChannelReference channelReference, long fileLength) {
        super(generation, channelReference, 0, fileLength);
    }

    @Override
    protected Translog.Operation read(BufferedChecksumStreamInput in) throws IOException {
        // read the opsize before an operation.
        // Note that this was written & read out side of the stream when this class was used, but it makes things more consistent
        // to read this here
        in.readInt();
        Translog.Operation.Type type = Translog.Operation.Type.fromId(in.readByte());
        Translog.Operation operation = Translog.newOperationFromType(type);
        operation.readFrom(in);
        return operation;
    }



    @Override
    protected ImmutableTranslogReader newReader(long generation, ChannelReference channelReference, long firstOperationOffset, long length, int totalOperations) {
        assert totalOperations == -1 : "expected unknown but was: " + totalOperations;
        assert firstOperationOffset == 0;
        return new LegacyTranslogReader(generation, channelReference, length);
    }
}

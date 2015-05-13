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

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Version 1 of the translog format, there is checkpoint and therefore no notion of op count
 */
@Deprecated
class LegacyTranslogReaderBase extends ImmutableTranslogReader {

    /**
     * Create a snapshot of translog file channel. The length parameter should be consistent with totalOperations and point
     * at the end of the last operation in this snapshot.
     *
     */
    LegacyTranslogReaderBase(long generation, ChannelReference channelReference, long firstOperationOffset, long fileLength) {
        super(generation, channelReference, firstOperationOffset, fileLength, TranslogReader.UNKNOWN_OP_COUNT);
    }


    @Override
    protected Translog.Snapshot newReaderSnapshot(final int totalOperations, ByteBuffer reusableBuffer) {
        assert totalOperations == -1 : "legacy we had no idea how many ops: " + totalOperations;
        return new ReaderSnapshot(totalOperations, reusableBuffer) {
            @Override
            public Translog.Operation next() throws IOException {
                if (position >= sizeInBytes()) { // this is the legacy case....
                    return null;
                }
                try {
                    return readOperation();
                } catch (TruncatedTranslogException ex) {
                    return null; // legacy case
                }
            }
        };
    }

    @Override
    protected ImmutableTranslogReader newReader(long generation, ChannelReference channelReference, long firstOperationOffset, long length, int totalOperations)  {
        assert totalOperations == -1 : "expected unknown but was: " + totalOperations;
        return new LegacyTranslogReaderBase(generation, channelReference, firstOperationOffset, length);
    }
}

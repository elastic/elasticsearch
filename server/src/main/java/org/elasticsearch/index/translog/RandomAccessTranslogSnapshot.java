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

import com.carrotsearch.hppc.LongLongMap;

import java.io.IOException;

/**
 * A random-access iterator over a fixed snapshot of a single translog generation.
 */
final class RandomAccessTranslogSnapshot extends TranslogSnapshotReader {

    private final LongLongMap index;

    /**
     * Create a snapshot of the translog file channel. This gives a random-access iterator over the operations in the snapshot, indexed by
     * the specified map from sequence number to position.
     *
     * @param reader the underlying reader
     * @param length the size in bytes of the underlying snapshot
     * @param index  the random-access index from sequence number to position for this snapshot
     */
    RandomAccessTranslogSnapshot(final BaseTranslogReader reader, final long length, final LongLongMap index) {
        super(reader, length);
        this.index = index;
    }

    Translog.Operation operation(final long seqNo) throws IOException {
        if (index.containsKey(seqNo)) {
            return readOperation(index.get(seqNo));
        } else {
            return null;
        }
    }

}

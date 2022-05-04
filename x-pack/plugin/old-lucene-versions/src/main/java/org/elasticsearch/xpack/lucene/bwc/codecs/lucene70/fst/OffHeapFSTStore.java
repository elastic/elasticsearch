/*
 * @notice
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Modifications copyright (C) 2021 Elasticsearch B.V.
 */
package org.elasticsearch.xpack.lucene.bwc.codecs.lucene70.fst;

import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.RamUsageEstimator;

import java.io.IOException;

/**
 * Provides off heap storage of finite state machine (FST), using underlying index input instead of
 * byte store on heap
 *
 */
public final class OffHeapFSTStore implements FSTStore {

    private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(OffHeapFSTStore.class);

    private IndexInput in;
    private long offset;
    private long numBytes;

    @Override
    public void init(DataInput in, long numBytes) throws IOException {
        if (in instanceof IndexInput) {
            this.in = (IndexInput) in;
            this.numBytes = numBytes;
            this.offset = this.in.getFilePointer();
        } else {
            throw new IllegalArgumentException(
                "parameter:in should be an instance of IndexInput for using OffHeapFSTStore, not a " + in.getClass().getName()
            );
        }
    }

    @Override
    public long ramBytesUsed() {
        return BASE_RAM_BYTES_USED;
    }

    @Override
    public long size() {
        return numBytes;
    }

    @Override
    public FST.BytesReader getReverseBytesReader() {
        try {
            return new ReverseRandomAccessReader(in.randomAccessSlice(offset, numBytes));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void writeTo(DataOutput out) throws IOException {
        throw new UnsupportedOperationException("writeToOutput operation is not supported for OffHeapFSTStore");
    }
}

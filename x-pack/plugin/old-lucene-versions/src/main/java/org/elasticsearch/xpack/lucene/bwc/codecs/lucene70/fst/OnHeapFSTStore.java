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
import org.apache.lucene.util.RamUsageEstimator;

import java.io.IOException;

/**
 * Provides storage of finite state machine (FST), using byte array or byte store allocated on heap.
 *
 */
public final class OnHeapFSTStore implements FSTStore {

    private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(OnHeapFSTStore.class);

    /**
     * A {@link BytesStore}, used during building, or during reading when the FST is very large (more
     * than 1 GB). If the FST is less than 1 GB then bytesArray is set instead.
     */
    private BytesStore bytes;

    /** Used at read time when the FST fits into a single byte[]. */
    private byte[] bytesArray;

    private final int maxBlockBits;

    public OnHeapFSTStore(int maxBlockBits) {
        if (maxBlockBits < 1 || maxBlockBits > 30) {
            throw new IllegalArgumentException("maxBlockBits should be 1 .. 30; got " + maxBlockBits);
        }

        this.maxBlockBits = maxBlockBits;
    }

    @Override
    public void init(DataInput in, long numBytes) throws IOException {
        if (numBytes > 1 << this.maxBlockBits) {
            // FST is big: we need multiple pages
            bytes = new BytesStore(in, numBytes, 1 << this.maxBlockBits);
        } else {
            // FST fits into a single block: use ByteArrayBytesStoreReader for less overhead
            bytesArray = new byte[(int) numBytes];
            in.readBytes(bytesArray, 0, bytesArray.length);
        }
    }

    @Override
    public long size() {
        if (bytesArray != null) {
            return bytesArray.length;
        } else {
            return bytes.ramBytesUsed();
        }
    }

    @Override
    public long ramBytesUsed() {
        return BASE_RAM_BYTES_USED + size();
    }

    @Override
    public FST.BytesReader getReverseBytesReader() {
        if (bytesArray != null) {
            return new ReverseBytesReader(bytesArray);
        } else {
            return bytes.getReverseReader();
        }
    }

    @Override
    public void writeTo(DataOutput out) throws IOException {
        if (bytes != null) {
            long numBytes = bytes.getPosition();
            out.writeVLong(numBytes);
            bytes.writeTo(out);
        } else {
            assert bytesArray != null;
            out.writeVLong(bytesArray.length);
            out.writeBytes(bytesArray, 0, bytesArray.length);
        }
    }
}

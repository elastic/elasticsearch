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
package org.elasticsearch.xpack.lucene.bwc.codecs.lucene60;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.bkd.BKDConfig;

import java.io.IOException;

/**
 * BKD Reader that only provides access to the metadata
 */
public class MetadataOnlyBKDReader extends PointValues {

    public static final int VERSION_START = 0;
    public static final int VERSION_SELECTIVE_INDEXING = 6;
    public static final int VERSION_META_FILE = 9;
    public static final int VERSION_CURRENT = VERSION_META_FILE;

    final BKDConfig config;
    final int numLeaves;
    final byte[] minPackedValue;
    final byte[] maxPackedValue;
    final long pointCount;
    final int docCount;
    final int version;

    public MetadataOnlyBKDReader(IndexInput metaIn) throws IOException {
        version = CodecUtil.checkHeader(metaIn, "BKD", VERSION_START, VERSION_CURRENT);
        final int numDims = metaIn.readVInt();
        final int numIndexDims;
        if (version >= VERSION_SELECTIVE_INDEXING) {
            numIndexDims = metaIn.readVInt();
        } else {
            numIndexDims = numDims;
        }
        final int maxPointsInLeafNode = metaIn.readVInt();
        final int bytesPerDim = metaIn.readVInt();
        config = new BKDConfig(numDims, numIndexDims, bytesPerDim, maxPointsInLeafNode);

        numLeaves = metaIn.readVInt();
        assert numLeaves > 0;

        minPackedValue = new byte[config.packedIndexBytesLength];
        maxPackedValue = new byte[config.packedIndexBytesLength];

        metaIn.readBytes(minPackedValue, 0, config.packedIndexBytesLength);
        metaIn.readBytes(maxPackedValue, 0, config.packedIndexBytesLength);
        final ArrayUtil.ByteArrayComparator comparator = ArrayUtil.getUnsignedComparator(config.bytesPerDim);
        for (int dim = 0; dim < config.numIndexDims; dim++) {
            if (comparator.compare(minPackedValue, dim * config.bytesPerDim, maxPackedValue, dim * config.bytesPerDim) > 0) {
                throw new CorruptIndexException(
                    "minPackedValue "
                        + new BytesRef(minPackedValue)
                        + " is > maxPackedValue "
                        + new BytesRef(maxPackedValue)
                        + " for dim="
                        + dim,
                    metaIn
                );
            }
        }

        pointCount = metaIn.readVLong();
        docCount = metaIn.readVInt();
    }

    @Override
    public PointTree getPointTree() {
        throw new UnsupportedOperationException("only metadata operations allowed");
    }

    @Override
    public byte[] getMinPackedValue() {
        return minPackedValue;
    }

    @Override
    public byte[] getMaxPackedValue() {
        return maxPackedValue;
    }

    @Override
    public int getNumDimensions() {
        return config.numDims;
    }

    @Override
    public int getNumIndexDimensions() {
        return config.numIndexDims;
    }

    @Override
    public int getBytesPerDimension() {
        return config.bytesPerDim;
    }

    @Override
    public long size() {
        return pointCount;
    }

    @Override
    public int getDocCount() {
        return docCount;
    }
}

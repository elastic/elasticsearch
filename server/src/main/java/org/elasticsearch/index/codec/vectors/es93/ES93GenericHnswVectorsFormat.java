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
 * Modifications copyright (C) 2024 Elasticsearch B.V.
 */

package org.elasticsearch.index.codec.vectors.es93;

import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.KnnVectorsWriter;
import org.apache.lucene.codecs.hnsw.FlatVectorsFormat;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.elasticsearch.index.codec.vectors.AbstractHnswVectorsFormat;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutorService;

public abstract class ES93GenericHnswVectorsFormat extends AbstractHnswVectorsFormat {

    static final String META_CODEC_NAME = "ES93GenericHnswVectorsFormatMeta";
    static final String VECTOR_INDEX_CODEC_NAME = "ES93GenericHnswVectorsFormatIndex";
    static final String META_EXTENSION = "vem";
    static final String VECTOR_INDEX_EXTENSION = "vex";

    public static final int VERSION_START = 0;
    public static final int VERSION_GROUPVARINT = 1;
    public static final int VERSION_CURRENT = VERSION_GROUPVARINT;

    static final int DIRECT_MONOTONIC_BLOCK_SHIFT = 16;

    private final int writeVersion = VERSION_CURRENT;

    public ES93GenericHnswVectorsFormat(String name) {
        super(name);
    }

    public ES93GenericHnswVectorsFormat(String name, int maxConn, int beamWidth) {
        super(name, maxConn, beamWidth);
    }

    public ES93GenericHnswVectorsFormat(String name, int maxConn, int beamWidth, int numMergeWorkers, ExecutorService mergeExec) {
        super(name, maxConn, beamWidth, numMergeWorkers, mergeExec);
    }

    @Override
    protected final FlatVectorsFormat flatVectorsFormat() {
        return writeFlatVectorsFormat();
    }

    protected abstract FlatVectorsFormat writeFlatVectorsFormat();

    protected abstract Map<String, FlatVectorsFormat> supportedReadFlatVectorsFormats();

    @Override
    public final KnnVectorsWriter fieldsWriter(SegmentWriteState state) throws IOException {
        var flatFormat = writeFlatVectorsFormat();
        return new ES93GenericHnswVectorsWriter(
            state,
            maxConn,
            beamWidth,
            flatFormat.getName(),
            flatFormat.fieldsWriter(state),
            numMergeWorkers,
            mergeExec,
            writeVersion
        );
    }

    @Override
    public final KnnVectorsReader fieldsReader(SegmentReadState state) throws IOException {
        var readFormats = supportedReadFlatVectorsFormats();
        return new ES93GenericHnswVectorsReader(state, f -> {
            var format = readFormats.get(f);
            if (format == null) return null;
            return format.fieldsReader(state);
        });
    }

    @Override
    public String toString() {
        return getName()
            + "(name="
            + getName()
            + ", maxConn="
            + maxConn
            + ", beamWidth="
            + beamWidth
            + ", writeFlatVectorFormat="
            + writeFlatVectorsFormat()
            + ", readFlatVectorsFormats="
            + supportedReadFlatVectorsFormats().values()
            + ")";
    }
}

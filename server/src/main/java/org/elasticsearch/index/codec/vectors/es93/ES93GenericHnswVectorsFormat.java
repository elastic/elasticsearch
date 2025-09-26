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
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsReader;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsWriter;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.elasticsearch.index.codec.vectors.AbstractHnswVectorsFormat;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutorService;

public class ES93GenericHnswVectorsFormat extends AbstractHnswVectorsFormat {

    static final String NAME = "ES93GenericHnswVectorsFormat";
    static final String VECTOR_FORMAT_INFO_EXTENSION = "vfi";
    static final String META_CODEC_NAME = "ES93GenericVectorsFormatMeta";

    public static final int VERSION_START = 0;
    public static final int VERSION_GROUPVARINT = 1;
    public static final int VERSION_CURRENT = VERSION_GROUPVARINT;

    static final Map<String, FlatVectorsFormat> availableFormats;

    static {
        ES93BinaryQuantizedVectorsFormat bbqFormat = new ES93BinaryQuantizedVectorsFormat(false);
        ES93BinaryQuantizedVectorsFormat bbqFormatDirectIO = new ES93BinaryQuantizedVectorsFormat(true);
        // ES815BitFlatVectorsFormat would go here too
        availableFormats = Map.of(bbqFormat.getName(), bbqFormat, bbqFormatDirectIO.getName(), bbqFormatDirectIO);
    }

    private final FlatVectorsFormat flatVectorsFormat;

    public ES93GenericHnswVectorsFormat() {
        super(NAME);
        flatVectorsFormat = null;
    }

    public ES93GenericHnswVectorsFormat(FlatVectorsFormat flatVectorsFormat) {
        super(NAME);
        this.flatVectorsFormat = flatVectorsFormat;
        if (availableFormats.containsKey(flatVectorsFormat.getName()) == false) {
            throw new IllegalArgumentException("Unknown flat format " + flatVectorsFormat.getName());
        }
    }

    public ES93GenericHnswVectorsFormat(int maxConn, int beamWidth) {
        super(NAME, maxConn, beamWidth);
        this.flatVectorsFormat = null;
    }

    public ES93GenericHnswVectorsFormat(int maxConn, int beamWidth, FlatVectorsFormat flatVectorsFormat) {
        super(NAME, maxConn, beamWidth);
        this.flatVectorsFormat = flatVectorsFormat;
        if (availableFormats.containsKey(flatVectorsFormat.getName()) == false) {
            throw new IllegalArgumentException("Unknown flat format " + flatVectorsFormat.getName());
        }
    }

    public ES93GenericHnswVectorsFormat(
        int maxConn,
        int beamWidth,
        int numMergeWorkers,
        ExecutorService mergeExec,
        FlatVectorsFormat flatVectorsFormat
    ) {
        super(NAME, maxConn, beamWidth, numMergeWorkers, mergeExec);
        this.flatVectorsFormat = flatVectorsFormat;
        if (availableFormats.containsKey(flatVectorsFormat.getName()) == false) {
            throw new IllegalArgumentException("Unknown flat format " + flatVectorsFormat.getName());
        }
    }

    @Override
    protected final FlatVectorsFormat flatVectorsFormat() {
        return flatVectorsFormat;
    }

    @Override
    public final KnnVectorsWriter fieldsWriter(SegmentWriteState state) throws IOException {
        if (flatVectorsFormat == null) {
            throw new IllegalStateException("Flat vector format to write with not specified");
        }
        return new Lucene99HnswVectorsWriter(
            state,
            maxConn,
            beamWidth,
            new ES93GenericFlatVectorsWriter(flatVectorsFormat.getName(), state, flatVectorsFormat.fieldsWriter(state)),
            numMergeWorkers,
            mergeExec
        );
    }

    @Override
    public final KnnVectorsReader fieldsReader(SegmentReadState state) throws IOException {
        // reads the format to use from disk, ignores flatVectorsFormat field
        return new Lucene99HnswVectorsReader(state, new ES93GenericFlatVectorsReader(state, f -> {
            var format = availableFormats.get(f);
            if (format == null) return null;
            return format.fieldsReader(state);
        }));
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
            + flatVectorsFormat
            + ")";
    }
}

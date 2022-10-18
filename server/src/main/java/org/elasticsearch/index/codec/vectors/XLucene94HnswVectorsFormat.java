/*
 * @notice
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.elasticsearch.index.codec.vectors;

import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.KnnVectorsWriter;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.util.hnsw.HnswGraph;

import java.io.IOException;

/**
 * Lucene 9.4 vector format, which encodes numeric vector values and an optional associated graph
 * connecting the documents having values. The graph is used to power HNSW search. The format
 * consists of three files:
 *
 * <h2>.vec (vector data) file</h2>
 *
 * <p>For each field:
 *
 * <ul>
 *   <li>Vector data ordered by field, document ordinal, and vector dimension. When the
 *       vectorEncoding is BYTE, each sample is stored as a single byte. When it is FLOAT32, each
 *       sample is stored as an IEEE float in little-endian byte order.
 *   <li>DocIds encoded by {@code IndexedDISI#writeBitSet(DocIdSetIterator, IndexOutput, byte)},
 *       note that only in sparse case
 *   <li>OrdToDoc was encoded by {@link org.apache.lucene.util.packed.DirectMonotonicWriter}, note
 *       that only in sparse case
 * </ul>
 *
 * <h2>.vex (vector index)</h2>
 *
 * <p>Stores graphs connecting the documents for each field organized as a list of nodes' neighbours
 * as following:
 *
 * <ul>
 *   <li>For each level:
 *       <ul>
 *         <li>For each node:
 *             <ul>
 *               <li><b>[int32]</b> the number of neighbor nodes
 *               <li><b>array[int32]</b> the neighbor ordinals
 *               <li><b>array[int32]</b> padding if the number of the node's neighbors is less than
 *                   the maximum number of connections allowed on this level. Padding is equal to
 *                   ((maxConnOnLevel – the number of neighbours) * 4) bytes.
 *             </ul>
 *       </ul>
 * </ul>
 *
 * <h2>.vem (vector metadata) file</h2>
 *
 * <p>For each field:
 *
 * <ul>
 *   <li><b>[int32]</b> field number
 *   <li><b>[int32]</b> vector similarity function ordinal
 *   <li><b>[vlong]</b> offset to this field's vectors in the .vec file
 *   <li><b>[vlong]</b> length of this field's vectors, in bytes
 *   <li><b>[vlong]</b> offset to this field's index in the .vex file
 *   <li><b>[vlong]</b> length of this field's index data, in bytes
 *   <li><b>[int]</b> dimension of this field's vectors
 *   <li><b>[int]</b> the number of documents having values for this field
 *   <li><b>[int8]</b> if equals to -1, dense – all documents have values for a field. If equals to
 *       0, sparse – some documents missing values.
 *   <li>DocIds were encoded by {@code IndexedDISI#writeBitSet(DocIdSetIterator, IndexOutput, byte)}
 *   <li>OrdToDoc was encoded by {@link org.apache.lucene.util.packed.DirectMonotonicWriter}, note
 *       that only in sparse case
 *   <li><b>[int]</b> the maximum number of connections (neigbours) that each node can have
 *   <li><b>[int]</b> number of levels in the graph
 *   <li>Graph nodes by level. For each level
 *       <ul>
 *         <li><b>[int]</b> the number of nodes on this level
 *         <li><b>array[int]</b> for levels greater than 0 list of nodes on this level, stored as
 *             the level 0th nodes' ordinals.
 *       </ul>
 * </ul>
 *
 * NOTE: this class was temporarily copied from Lucene to fix a bug in Lucene94HnswVectorsReader.
 * It contains no modifications to the Lucene version.
 */
public final class XLucene94HnswVectorsFormat extends KnnVectorsFormat {

    static final String META_CODEC_NAME = "lucene94HnswVectorsFormatMeta";
    static final String VECTOR_DATA_CODEC_NAME = "lucene94HnswVectorsFormatData";
    static final String VECTOR_INDEX_CODEC_NAME = "lucene94HnswVectorsFormatIndex";
    static final String META_EXTENSION = "vem";
    static final String VECTOR_DATA_EXTENSION = "vec";
    static final String VECTOR_INDEX_EXTENSION = "vex";

    public static final int VERSION_START = 0;
    public static final int VERSION_CURRENT = 1;

    /** Default number of maximum connections per node */
    public static final int DEFAULT_MAX_CONN = 16;
    /**
     * Default number of the size of the queue maintained while searching during a graph construction.
     */
    public static final int DEFAULT_BEAM_WIDTH = 100;

    static final int DIRECT_MONOTONIC_BLOCK_SHIFT = 16;

    /**
     * Controls how many of the nearest neighbor candidates are connected to the new node. Defaults to
     * {@link XLucene94HnswVectorsFormat#DEFAULT_MAX_CONN}. See {@link HnswGraph} for more details.
     */
    private final int maxConn;

    /**
     * The number of candidate neighbors to track while searching the graph for each newly inserted
     * node. Defaults to to {@link XLucene94HnswVectorsFormat#DEFAULT_BEAM_WIDTH}. See {@link
     * HnswGraph} for details.
     */
    private final int beamWidth;

    /** Constructs a format using default graph construction parameters */
    public XLucene94HnswVectorsFormat() {
        this(DEFAULT_MAX_CONN, DEFAULT_BEAM_WIDTH);
    }

    /**
     * Constructs a format using the given graph construction parameters.
     *
     * @param maxConn the maximum number of connections to a node in the HNSW graph
     * @param beamWidth the size of the queue maintained during graph construction.
     */
    public XLucene94HnswVectorsFormat(int maxConn, int beamWidth) {
        super("Lucene94HnswVectorsFormat");
        this.maxConn = maxConn;
        this.beamWidth = beamWidth;
    }

    @Override
    public KnnVectorsWriter fieldsWriter(SegmentWriteState state) throws IOException {
        return new XLucene94HnswVectorsWriter(state, maxConn, beamWidth);
    }

    @Override
    public KnnVectorsReader fieldsReader(SegmentReadState state) throws IOException {
        return new XLucene94HnswVectorsReader(state);
    }

    @Override
    public String toString() {
        return "Lucene94HnswVectorsFormat(name=Lucene94HnswVectorsFormat, maxConn=" + maxConn + ", beamWidth=" + beamWidth + ")";
    }
}

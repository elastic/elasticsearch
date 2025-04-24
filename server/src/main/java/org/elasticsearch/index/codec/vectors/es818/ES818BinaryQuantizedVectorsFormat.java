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
package org.elasticsearch.index.codec.vectors.es818;

import org.apache.lucene.codecs.hnsw.FlatVectorScorerUtil;
import org.apache.lucene.codecs.hnsw.FlatVectorsFormat;
import org.apache.lucene.codecs.hnsw.FlatVectorsReader;
import org.apache.lucene.codecs.hnsw.FlatVectorsWriter;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.elasticsearch.index.codec.vectors.OptimizedScalarQuantizer;

import java.io.IOException;

import static org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.MAX_DIMS_COUNT;

/**
 * Copied from Lucene, replace with Lucene's implementation sometime after Lucene 10
 * Codec for encoding/decoding binary quantized vectors The binary quantization format used here
 * is a per-vector optimized scalar quantization. Also see {@link
 * OptimizedScalarQuantizer}. Some of key features are:
 *
 * <ul>
 *   <li>Estimating the distance between two vectors using their centroid normalized distance. This
 *       requires some additional corrective factors, but allows for centroid normalization to occur.
 *   <li>Optimized scalar quantization to bit level of centroid normalized vectors.
 *   <li>Asymmetric quantization of vectors, where query vectors are quantized to half-byte
 *       precision (normalized to the centroid) and then compared directly against the single bit
 *       quantized vectors in the index.
 *   <li>Transforming the half-byte quantized query vectors in such a way that the comparison with
 *       single bit vectors can be done with bit arithmetic.
 * </ul>
 *
 * The format is stored in two files:
 *
 * <h2>.veb (vector data) file</h2>
 *
 * <p>Stores the binary quantized vectors in a flat format. Additionally, it stores each vector's
 * corrective factors. At the end of the file, additional information is stored for vector ordinal
 * to centroid ordinal mapping and sparse vector information.
 *
 * <ul>
 *   <li>For each vector:
 *       <ul>
 *         <li><b>[byte]</b> the binary quantized values, each byte holds 8 bits.
 *         <li><b>[float]</b> the optimized quantiles and an additional similarity dependent corrective factor.
 *         <li><b>short</b> the sum of the quantized components </li>
 *       </ul>
 *   <li>After the vectors, sparse vector information keeping track of monotonic blocks.
 * </ul>
 *
 * <h2>.vemb (vector metadata) file</h2>
 *
 * <p>Stores the metadata for the vectors. This includes the number of vectors, the number of
 * dimensions, and file offset information.
 *
 * <ul>
 *   <li><b>int</b> the field number
 *   <li><b>int</b> the vector encoding ordinal
 *   <li><b>int</b> the vector similarity ordinal
 *   <li><b>vint</b> the vector dimensions
 *   <li><b>vlong</b> the offset to the vector data in the .veb file
 *   <li><b>vlong</b> the length of the vector data in the .veb file
 *   <li><b>vint</b> the number of vectors
 *   <li><b>[float]</b> the centroid </li>
 *   <li><b>float</b> the centroid square magnitude </li>
  *  <li>The sparse vector information, if required, mapping vector ordinal to doc ID
  * </ul>
 */
public class ES818BinaryQuantizedVectorsFormat extends FlatVectorsFormat {

    public static final String BINARIZED_VECTOR_COMPONENT = "BVEC";
    public static final String NAME = "ES818BinaryQuantizedVectorsFormat";

    static final int VERSION_START = 0;
    static final int VERSION_CURRENT = VERSION_START;
    static final String META_CODEC_NAME = "ES818BinaryQuantizedVectorsFormatMeta";
    static final String VECTOR_DATA_CODEC_NAME = "ES818BinaryQuantizedVectorsFormatData";
    static final String META_EXTENSION = "vemb";
    static final String VECTOR_DATA_EXTENSION = "veb";
    static final int DIRECT_MONOTONIC_BLOCK_SHIFT = 16;

    private static final DirectIOLucene99FlatVectorsFormat rawVectorFormat = new DirectIOLucene99FlatVectorsFormat(
        FlatVectorScorerUtil.getLucene99FlatVectorsScorer()
    );

    private static final ES818BinaryFlatVectorsScorer scorer = new ES818BinaryFlatVectorsScorer(
        FlatVectorScorerUtil.getLucene99FlatVectorsScorer()
    );

    /** Creates a new instance with the default number of vectors per cluster. */
    public ES818BinaryQuantizedVectorsFormat() {
        super(NAME);
    }

    @Override
    public FlatVectorsWriter fieldsWriter(SegmentWriteState state) throws IOException {
        return new ES818BinaryQuantizedVectorsWriter(scorer, rawVectorFormat.fieldsWriter(state), state);
    }

    @Override
    public FlatVectorsReader fieldsReader(SegmentReadState state) throws IOException {
        return new ES818BinaryQuantizedVectorsReader(state, rawVectorFormat.fieldsReader(state), scorer);
    }

    @Override
    public int getMaxDimensions(String fieldName) {
        return MAX_DIMS_COUNT;
    }

    @Override
    public String toString() {
        return "ES818BinaryQuantizedVectorsFormat(name=" + NAME + ", flatVectorScorer=" + scorer + ")";
    }
}

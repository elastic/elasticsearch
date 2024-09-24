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
package org.elasticsearch.index.codec.vectors;

import org.apache.lucene.codecs.hnsw.FlatVectorsScorer;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;

import java.io.IOException;

/** An interface of flat vector scorer over binarized vector values */
public interface BinaryFlatVectorsScorer extends FlatVectorsScorer {

    /**
     * @param similarityFunction vector similarity function
     * @param scoringVectors the vectors over which to score
     * @param targetVectors the target vectors
     * @return a {@link RandomVectorScorerSupplier} that can be used to score vectors
     * @throws IOException if an I/O error occurs
     */
    RandomVectorScorerSupplier getRandomVectorScorerSupplier(
        VectorSimilarityFunction similarityFunction,
        RandomAccessBinarizedQueryByteVectorValues scoringVectors,
        RandomAccessBinarizedByteVectorValues targetVectors
    ) throws IOException;
}

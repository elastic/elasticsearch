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

import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.search.VectorScorer;
import org.apache.lucene.util.VectorUtil;
import org.elasticsearch.index.codec.vectors.BQVectorUtils;
import org.elasticsearch.index.codec.vectors.OptimizedScalarQuantizer;

import java.io.IOException;

/**
 * Copied from Lucene, replace with Lucene's implementation sometime after Lucene 10
 */
abstract class BinarizedByteVectorValues extends ByteVectorValues {

    /**
     * Retrieve the corrective terms for the given vector ordinal. For the dot-product family of
     * distances, the corrective terms are, in order
     *
     * <ul>
     *   <li>the lower optimized interval
     *   <li>the upper optimized interval
     *   <li>the dot-product of the non-centered vector with the centroid
     *   <li>the sum of quantized components
     * </ul>
     *
     * For euclidean:
     *
     * <ul>
     *   <li>the lower optimized interval
     *   <li>the upper optimized interval
     *   <li>the l2norm of the centered vector
     *   <li>the sum of quantized components
     * </ul>
     *
     * @param vectorOrd the vector ordinal
     * @return the corrective terms
     * @throws IOException if an I/O error occurs
     */
    public abstract OptimizedScalarQuantizer.QuantizationResult getCorrectiveTerms(int vectorOrd) throws IOException;

    /**
     * @return the quantizer used to quantize the vectors
     */
    public abstract OptimizedScalarQuantizer getQuantizer();

    public abstract float[] getCentroid() throws IOException;

    int discretizedDimensions() {
        return BQVectorUtils.discretize(dimension(), 64);
    }

    /**
     * Return a {@link VectorScorer} for the given query vector.
     *
     * @param query the query vector
     * @return a {@link VectorScorer} instance or null
     */
    public abstract VectorScorer scorer(float[] query) throws IOException;

    @Override
    public abstract BinarizedByteVectorValues copy() throws IOException;

    float getCentroidDP() throws IOException {
        // this only gets executed on-merge
        float[] centroid = getCentroid();
        return VectorUtil.dotProduct(centroid, centroid);
    }
}

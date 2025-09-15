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

import org.apache.lucene.util.VectorUtil;
import org.apache.lucene.util.hnsw.RandomAccessVectorValues;
import org.elasticsearch.index.codec.vectors.BQVectorUtils;
import org.elasticsearch.index.codec.vectors.OptimizedScalarQuantizer;

import java.io.IOException;

/**
 * Copied from Lucene, replace with Lucene's implementation sometime after Lucene 10
 */
public interface RandomAccessBinarizedByteVectorValues extends RandomAccessVectorValues.Bytes {
    OptimizedScalarQuantizer.QuantizationResult getCorrectiveTerms(int vectorOrd) throws IOException;

    /**
     * @return the quantizer used to quantize the vectors
     */
    OptimizedScalarQuantizer getQuantizer();

    default int discretizedDimensions() {
        return BQVectorUtils.discretize(dimension(), 64);
    }

    /**
     * @return coarse grained centroids for the vectors
     */
    float[] getCentroid() throws IOException;

    @Override
    RandomAccessBinarizedByteVectorValues copy() throws IOException;

    default float getCentroidDP() throws IOException {
        // this only gets executed on-merge
        float[] centroid = getCentroid();
        return VectorUtil.dotProduct(centroid, centroid);
    }
}

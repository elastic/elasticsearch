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

import org.apache.lucene.util.VectorUtil;
import org.apache.lucene.util.hnsw.RandomAccessVectorValues;

import java.io.IOException;

import static org.elasticsearch.index.codec.vectors.BQVectorUtils.constSqrt;

/**
 * Copied from Lucene, replace with Lucene's implementation sometime after Lucene 10
 */
public interface RandomAccessBinarizedByteVectorValues extends RandomAccessVectorValues.Bytes {
    /** Returns the centroid distance for the vector */
    float getCentroidDistance(int vectorOrd) throws IOException;

    /** Returns the vector magnitude for the vector */
    float getVectorMagnitude(int vectorOrd) throws IOException;

    /** Returns OOQ corrective factor for the given vector ordinal */
    float getOOQ(int targetOrd) throws IOException;

    /**
     * Returns the norm of the target vector w the centroid corrective factor for the given vector
     * ordinal
     */
    float getNormOC(int targetOrd) throws IOException;

    /**
     * Returns the target vector dot product the centroid corrective factor for the given vector
     * ordinal
     */
    float getODotC(int targetOrd) throws IOException;

    /**
     * @return the quantizer used to quantize the vectors
     */
    BinaryQuantizer getQuantizer();

    default int discretizedDimensions() {
        return BQVectorUtils.discretize(dimension(), 64);
    }

    default float sqrtDimensions() {
        return (float) constSqrt(dimension());
    }

    default float maxX1() {
        return (float) (1.9 / constSqrt(discretizedDimensions() - 1.0));
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

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

import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.search.VectorScorer;
import org.apache.lucene.util.VectorUtil;

import java.io.IOException;

import static org.elasticsearch.index.codec.vectors.BQVectorUtils.constSqrt;

/**
 * Copied from Lucene, replace with Lucene's implementation sometime after Lucene 10
 */
public abstract class BinarizedByteVectorValues extends ByteVectorValues {

    public abstract float[] getCorrectiveTerms(int vectorOrd) throws IOException;

    /** Return the dimension of the vectors */
    public abstract int dimension();

    /** Returns the centroid distance for the vector */
    public abstract float getCentroidDistance(int vectorOrd) throws IOException;

    /** Returns the vector magnitude for the vector */
    public abstract float getVectorMagnitude(int vectorOrd) throws IOException;

    /** Returns OOQ corrective factor for the given vector ordinal */
    public abstract float getOOQ(int targetOrd) throws IOException;

    /**
     * Returns the norm of the target vector w the centroid corrective factor for the given vector
     * ordinal
     */
    public abstract float getNormOC(int targetOrd) throws IOException;

    /**
     * Returns the target vector dot product the centroid corrective factor for the given vector
     * ordinal
     */
    public abstract float getODotC(int targetOrd) throws IOException;

    /**
     * @return the quantizer used to quantize the vectors
     */
    public abstract BinaryQuantizer getQuantizer();

    public abstract float[] getCentroid() throws IOException;

    /**
     * Return the number of vectors for this field.
     *
     * @return the number of vectors returned by this iterator
     */
    public abstract int size();

    int discretizedDimensions() {
        return BQVectorUtils.discretize(dimension(), 64);
    }

    float sqrtDimensions() {
        return (float) constSqrt(dimension());
    }

    float maxX1() {
        return (float) (1.9 / constSqrt(discretizedDimensions() - 1.0));
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

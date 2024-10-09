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

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.VectorScorer;

import java.io.IOException;

/**
 * Copied from Lucene, replace with Lucene's implementation sometime after Lucene 10
 */
public abstract class BinarizedByteVectorValues extends DocIdSetIterator {

    public abstract float[] getCorrectiveTerms();

    public abstract byte[] vectorValue() throws IOException;

    /** Return the dimension of the vectors */
    public abstract int dimension();

    /**
     * Return the number of vectors for this field.
     *
     * @return the number of vectors returned by this iterator
     */
    public abstract int size();

    @Override
    public final long cost() {
        return size();
    }

    /**
     * Return a {@link VectorScorer} for the given query vector.
     *
     * @param query the query vector
     * @return a {@link VectorScorer} instance or null
     */
    public abstract VectorScorer scorer(float[] query) throws IOException;
}

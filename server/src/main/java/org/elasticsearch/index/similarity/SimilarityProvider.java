/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.similarity;

import org.apache.lucene.search.similarities.Similarity;

import java.util.Objects;

/**
 * Wrapper around a {@link Similarity} and its name.
 */
public final class SimilarityProvider {

    private final String name;
    private final Similarity similarity;

    public SimilarityProvider(String name, Similarity similarity) {
        this.name = name;
        this.similarity = similarity;
    }

    /**
     * Return the name of this {@link Similarity}.
     */
    public String name() {
        return name;
    }

    /**
     * Return the wrapped {@link Similarity}.
     */
    public Similarity get() {
        return similarity;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SimilarityProvider that = (SimilarityProvider) o;
        /**
         * We check <code>name</code> only because the <code>similarity</code> is
         * re-created for each new instance and they don't implement equals.
         * This is not entirely correct though but we only use equality checks
         * for similarities inside the same index and names are unique in this case.
         **/
        return Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
        /**
         * We use <code>name</code> only because the <code>similarity</code> is
         * re-created for each new instance and they don't implement equals.
         * This is not entirely correct though but we only use equality checks
         * for similarities a single index and names are unique in this case.
         **/
        return Objects.hash(name);
    }
}

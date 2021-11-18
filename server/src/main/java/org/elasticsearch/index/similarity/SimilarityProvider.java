/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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

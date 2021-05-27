/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.indices;

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Validatable;

import java.util.Objects;

/**
 * Request for the _reload_search_analyzers API
 */
public final class ReloadAnalyzersRequest implements Validatable {

    private final String[] indices;
    private IndicesOptions indicesOptions = IndicesOptions.strictExpandOpen();

    /**
     * Creates a new reload analyzers request
     * @param indices the index for which to reload analyzers
     */
    public ReloadAnalyzersRequest(String... indices) {
        this.indices = Objects.requireNonNull(indices);
    }

    /**
     * Returns the indices
     */
    public String[] getIndices() {
        return indices;
    }

    /**
     * Specifies what type of requested indices to ignore and how to deal with wildcard expressions.
     * For example indices that don't exist.
     *
     * @return the current behaviour when it comes to index names and wildcard indices expressions
     */
    public IndicesOptions indicesOptions() {
        return indicesOptions;
    }

    /**
     * Specifies what type of requested indices to ignore and how to deal with wildcard expressions.
     * For example indices that don't exist.
     *
     * @param indicesOptions the desired behaviour regarding indices to ignore and wildcard indices expressions
     */
    public void setIndicesOptions(IndicesOptions indicesOptions) {
        this.indicesOptions = indicesOptions;
    }
}

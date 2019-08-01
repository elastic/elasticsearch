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

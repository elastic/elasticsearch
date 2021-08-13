/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.ilm;

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.TimedRequest;
import org.elasticsearch.client.ValidationException;

import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;

/**
 * The request object used by the Explain Lifecycle API.
 */
public class ExplainLifecycleRequest extends TimedRequest {

    private final String[] indices;
    private IndicesOptions indicesOptions = IndicesOptions.strictExpandOpen();

    public ExplainLifecycleRequest(String... indices) {
        if (indices.length == 0) {
            throw new IllegalArgumentException("Must at least specify one index to explain");
        }
        this.indices = indices;
    }

    public String[] getIndices() {
        return indices;
    }

    public ExplainLifecycleRequest indicesOptions(IndicesOptions indicesOptions) {
        this.indicesOptions = indicesOptions;
        return this;
    }

    public IndicesOptions indicesOptions() {
        return indicesOptions;
    }

    @Override
    public Optional<ValidationException> validate() {
        return Optional.empty();
    }

    @Override
    public int hashCode() {
        return Objects.hash(Arrays.hashCode(indices), indicesOptions);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj.getClass() != getClass()) {
            return false;
        }
        ExplainLifecycleRequest other = (ExplainLifecycleRequest) obj;
        return Objects.deepEquals(getIndices(), other.getIndices()) &&
                Objects.equals(indicesOptions(), other.indicesOptions());
    }

    @Override
    public String toString() {
        return "ExplainLifecycleRequest [indices()=" + Arrays.toString(indices) + ", indicesOptions()=" + indicesOptions + "]";
    }

}

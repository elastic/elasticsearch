/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.ilm;

import org.elasticsearch.client.TimedRequest;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class RetryLifecyclePolicyRequest extends TimedRequest {

    private final List<String> indices;

    public RetryLifecyclePolicyRequest(String... indices) {
        if (indices.length == 0) {
            throw new IllegalArgumentException("Must at least specify one index to retry");
        }
        this.indices = Arrays.asList(indices);
    }

    public List<String> getIndices() {
        return indices;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RetryLifecyclePolicyRequest that = (RetryLifecyclePolicyRequest) o;
        return indices.size() == that.indices.size() && indices.containsAll(that.indices);
    }

    @Override
    public int hashCode() {
        return Objects.hash(indices);
    }
}

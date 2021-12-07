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

import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class RemoveIndexLifecyclePolicyRequest extends TimedRequest {

    private final List<String> indices;
    private final IndicesOptions indicesOptions;

    public RemoveIndexLifecyclePolicyRequest(List<String> indices) {
        this.indices = Objects.requireNonNull(indices);
        this.indicesOptions = null;
    }

    public RemoveIndexLifecyclePolicyRequest(List<String> indices, IndicesOptions indicesOptions) {
        this.indices = Collections.unmodifiableList(Objects.requireNonNull(indices));
        this.indicesOptions = Objects.requireNonNull(indicesOptions);
    }

    public List<String> indices() {
        return indices;
    }

    public IndicesOptions indicesOptions() {
        return indicesOptions;
    }

    @Override
    public int hashCode() {
        return Objects.hash(indices, indicesOptions);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        RemoveIndexLifecyclePolicyRequest other = (RemoveIndexLifecyclePolicyRequest) obj;
        return Objects.deepEquals(indices, other.indices) && Objects.equals(indicesOptions, other.indicesOptions);
    }
}

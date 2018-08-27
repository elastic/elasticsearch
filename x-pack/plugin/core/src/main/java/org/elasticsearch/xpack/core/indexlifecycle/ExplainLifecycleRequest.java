/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.indexlifecycle;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.master.info.ClusterInfoRequest;
import org.elasticsearch.common.io.stream.StreamInput;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

/**
 * The request object used by the Explain Lifecycle API.
 * 
 * Multiple indices may be queried in the same request using the
 * {@link #indices(String...)} method
 */
public class ExplainLifecycleRequest extends ClusterInfoRequest<ExplainLifecycleRequest> {

    public ExplainLifecycleRequest() {
        super();
    }

    public ExplainLifecycleRequest(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    @Override
    public int hashCode() {
        return Objects.hash(Arrays.hashCode(indices()), indicesOptions());
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
        return Objects.deepEquals(indices(), other.indices()) &&
                Objects.equals(indicesOptions(), other.indicesOptions());
    }

    @Override
    public String toString() {
        return "ExplainLifecycleRequest [indices()=" + Arrays.toString(indices()) + ", indicesOptions()=" + indicesOptions() + "]";
    }

}

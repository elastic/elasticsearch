/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.master.info.ClusterInfoRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

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
    private static final Version FILTERS_INTRODUCED_VERSION = Version.V_7_4_0;

    private boolean onlyErrors = false;
    private boolean onlyManaged = false;

    public ExplainLifecycleRequest() {
        super();
    }

    public ExplainLifecycleRequest(StreamInput in) throws IOException {
        super(in);
        if (in.getVersion().onOrAfter(FILTERS_INTRODUCED_VERSION)) {
            onlyErrors = in.readBoolean();
            onlyManaged = in.readBoolean();
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        if (out.getVersion().onOrAfter(FILTERS_INTRODUCED_VERSION)) {
            out.writeBoolean(onlyErrors);
            out.writeBoolean(onlyManaged);
        }
    }

    public boolean onlyErrors() {
        return onlyErrors;
    }

    public ExplainLifecycleRequest onlyErrors(boolean onlyErrors) {
        this.onlyErrors = onlyErrors;
        return this;
    }

    public boolean onlyManaged() {
        return onlyManaged;
    }

    public ExplainLifecycleRequest onlyManaged(boolean onlyManaged) {
        this.onlyManaged = onlyManaged;
        return this;
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    @Override
    public int hashCode() {
        return Objects.hash(Arrays.hashCode(indices()), indicesOptions(), onlyErrors, onlyManaged);
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
                Objects.equals(indicesOptions(), other.indicesOptions()) &&
                Objects.equals(onlyErrors(), other.onlyErrors()) &&
                Objects.equals(onlyManaged(), other.onlyManaged());
    }

    @Override
    public String toString() {
        return "ExplainLifecycleRequest [indices()=" + Arrays.toString(indices()) + ", indicesOptions()=" + indicesOptions() +
            ", onlyErrors()=" + onlyErrors() + ", onlyManaged()=" + onlyManaged() + "]";
    }

}

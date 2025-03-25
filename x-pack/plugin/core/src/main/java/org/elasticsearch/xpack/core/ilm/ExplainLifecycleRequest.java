/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.local.LocalClusterStateRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.UpdateForV10;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;

/**
 * The request object used by the Explain Lifecycle API.
 * <p>
 * Multiple indices may be queried in the same request using the
 * {@link #indices(String...)} method
 */
public class ExplainLifecycleRequest extends LocalClusterStateRequest implements IndicesRequest.Replaceable {

    private String[] indices = Strings.EMPTY_ARRAY;
    private IndicesOptions indicesOptions;
    private boolean onlyErrors = false;
    private boolean onlyManaged = false;

    public ExplainLifecycleRequest(TimeValue masterTimeout) {
        super(masterTimeout);
        indicesOptions = IndicesOptions.strictExpandOpen();
    }

    /**
     * NB prior to 9.0 this was a TransportMasterNodeReadAction so for BwC we must remain able to read these requests until
     * we no longer need to support calling this action remotely.
     */
    @UpdateForV10(owner = UpdateForV10.Owner.DATA_MANAGEMENT)
    public ExplainLifecycleRequest(StreamInput in) throws IOException {
        super(in);
        indices = in.readStringArray();
        if (in.getTransportVersion().before(TransportVersions.V_8_0_0)) {
            in.readStringArray();
        }
        indicesOptions = IndicesOptions.readIndicesOptions(in);
        onlyErrors = in.readBoolean();
        onlyManaged = in.readBoolean();
    }

    @Override
    public ExplainLifecycleRequest indices(String... indices) {
        this.indices = indices;
        return this;
    }

    public ExplainLifecycleRequest indicesOptions(IndicesOptions indicesOptions) {
        this.indicesOptions = indicesOptions;
        return this;
    }

    @Override
    public String[] indices() {
        return indices;
    }

    @Override
    public IndicesOptions indicesOptions() {
        return indicesOptions;
    }

    @Override
    public boolean includeDataStreams() {
        return true;
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
    public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
        return new CancellableTask(id, type, action, "", parentTaskId, headers);
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
        return Objects.deepEquals(indices(), other.indices())
            && Objects.equals(indicesOptions(), other.indicesOptions())
            && Objects.equals(onlyErrors(), other.onlyErrors())
            && Objects.equals(onlyManaged(), other.onlyManaged());
    }

    @Override
    public String toString() {
        return "ExplainLifecycleRequest [indices()="
            + Arrays.toString(indices())
            + ", indicesOptions()="
            + indicesOptions()
            + ", onlyErrors()="
            + onlyErrors()
            + ", onlyManaged()="
            + onlyManaged()
            + "]";
    }

}

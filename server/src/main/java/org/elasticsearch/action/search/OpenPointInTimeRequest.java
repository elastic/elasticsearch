/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.search;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public final class OpenPointInTimeRequest extends ActionRequest implements IndicesRequest.Replaceable {
    private String[] indices;
    private IndicesOptions indicesOptions = DEFAULT_INDICES_OPTIONS;
    private TimeValue keepAlive;

    @Nullable
    private String routing;
    @Nullable
    private String preference;

    public static final IndicesOptions DEFAULT_INDICES_OPTIONS = IndicesOptions.strictExpandOpenAndForbidClosed();

    public OpenPointInTimeRequest(String... indices) {
        this.indices = Objects.requireNonNull(indices, "[index] is not specified");
    }

    public OpenPointInTimeRequest(StreamInput in) throws IOException {
        super(in);
        this.indices = in.readStringArray();
        this.indicesOptions = IndicesOptions.readIndicesOptions(in);
        this.keepAlive = in.readTimeValue();
        this.routing = in.readOptionalString();
        this.preference = in.readOptionalString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringArray(indices);
        indicesOptions.writeIndicesOptions(out);
        out.writeTimeValue(keepAlive);
        out.writeOptionalString(routing);
        out.writeOptionalString(preference);
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (indices == null || indices.length == 0) {
            validationException = addValidationError("[index] is not specified", validationException);
        }
        if (keepAlive == null) {
            validationException = addValidationError("[keep_alive] is not specified", validationException);
        }
        return validationException;
    }

    @Override
    public String[] indices() {
        return indices;
    }

    @Override
    public OpenPointInTimeRequest indices(String... indices) {
        this.indices = indices;
        return this;
    }

    @Override
    public IndicesOptions indicesOptions() {
        return indicesOptions;
    }

    public OpenPointInTimeRequest indicesOptions(IndicesOptions indicesOptions) {
        this.indicesOptions = Objects.requireNonNull(indicesOptions, "[indices_options] parameter must be non null");
        return this;
    }

    public TimeValue keepAlive() {
        return keepAlive;
    }

    /**
     * Set keep alive for the point in time
     */
    public OpenPointInTimeRequest keepAlive(TimeValue keepAlive) {
        this.keepAlive = Objects.requireNonNull(keepAlive, "[keep_alive] parameter must be non null");
        return this;
    }

    public String routing() {
        return routing;
    }

    public OpenPointInTimeRequest routing(String routing) {
        this.routing = routing;
        return this;
    }

    public String preference() {
        return preference;
    }

    public OpenPointInTimeRequest preference(String preference) {
        this.preference = preference;
        return this;
    }

    @Override
    public boolean allowsRemoteIndices() {
        return true;
    }

    @Override
    public boolean includeDataStreams() {
        return true;
    }

    @Override
    public String getDescription() {
        return "open search context: indices [" + String.join(",", indices) + "] keep_alive [" + keepAlive + "]";
    }

    @Override
    public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
        return new SearchTask(id, type, action, this::getDescription, parentTaskId, headers);
    }
}

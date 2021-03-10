/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.search.action;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.search.SearchTask;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public final class OpenPointInTimeRequest extends ActionRequest implements IndicesRequest.Replaceable {
    private String[] indices;
    private final IndicesOptions indicesOptions;
    private final TimeValue keepAlive;

    @Nullable
    private final String routing;
    @Nullable
    private final String preference;

    public static final IndicesOptions DEFAULT_INDICES_OPTIONS = IndicesOptions.strictExpandOpenAndForbidClosed();

    public OpenPointInTimeRequest(String[] indices, IndicesOptions indicesOptions,
                                  TimeValue keepAlive, String routing, String preference) {
        this.indices = Objects.requireNonNull(indices);
        this.indicesOptions = Objects.requireNonNull(indicesOptions);
        this.keepAlive = keepAlive;
        this.routing = routing;
        this.preference = preference;
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
        if (indices.length == 0) {
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

    @Override
    public boolean includeDataStreams() {
        return true;
    }

    public TimeValue keepAlive() {
        return keepAlive;
    }

    public String routing() {
        return routing;
    }

    public String preference() {
        return preference;
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

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;

import java.io.IOException;
import java.util.Map;

/**
 * Transport request for {@link EsqlResolveFieldsAction}, wrapping {@link FieldCapabilitiesRequest}
 * with ES|QL-specific parameters without modifying the original field-caps request type.
 */
public class EsqlResolveFieldsRequest extends ActionRequest {

    private final FieldCapabilitiesRequest fieldCapsRequest;
    private final int depth;

    public EsqlResolveFieldsRequest(FieldCapabilitiesRequest fieldCapsRequest, int depth) {
        this.fieldCapsRequest = fieldCapsRequest;
        this.depth = depth;
    }

    @SuppressWarnings("this-escape")
    public EsqlResolveFieldsRequest(StreamInput in) throws IOException {
        this.fieldCapsRequest = new FieldCapabilitiesRequest(in);
        setParentTask(fieldCapsRequest.getParentTask());
        this.depth = in.readVInt();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        fieldCapsRequest.writeTo(out);
        out.writeVInt(depth);
    }

    public FieldCapabilitiesRequest fieldCapsRequest() {
        return fieldCapsRequest;
    }

    public int depth() {
        return depth;
    }

    @Override
    public ActionRequestValidationException validate() {
        return fieldCapsRequest.validate();
    }

    @Override
    public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
        return new CancellableTask(id, type, action, "", parentTaskId, headers);
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.ResolvedIndexExpressions;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.crossproject.TargetProjects;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;

import java.io.IOException;
import java.util.Map;

/**
 * Transport request for {@link EsqlResolveFieldsAction}, currently wrapping {@link FieldCapabilitiesRequest}
 * with ES|QL-specific parameters without modifying the original field-caps request type.
 */
public class EsqlResolveFieldsRequest extends ActionRequest implements IndicesRequest.Replaceable {

    private final FieldCapabilitiesRequest fieldCapsRequest;

    public EsqlResolveFieldsRequest(FieldCapabilitiesRequest fieldCapsRequest) {
        this.fieldCapsRequest = fieldCapsRequest;
    }

    @SuppressWarnings("this-escape")
    public EsqlResolveFieldsRequest(StreamInput in) throws IOException {
        this.fieldCapsRequest = new FieldCapabilitiesRequest(in);
        setParentTask(fieldCapsRequest.getParentTask());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        fieldCapsRequest.writeTo(out);
    }

    public FieldCapabilitiesRequest fieldCapsRequest() {
        return fieldCapsRequest;
    }

    @Override
    public String[] indices() {
        return fieldCapsRequest.indices();
    }

    @Override
    public IndicesRequest indices(String... indices) {
        fieldCapsRequest.indices(indices);
        return this;
    }

    @Override
    public IndicesOptions indicesOptions() {
        return fieldCapsRequest.indicesOptions();
    }

    @Override
    public boolean includeDataStreams() {
        return fieldCapsRequest.includeDataStreams();
    }

    @Override
    public boolean allowsRemoteIndices() {
        return fieldCapsRequest.allowsRemoteIndices();
    }

    @Override
    public boolean allowsCrossProject() {
        return fieldCapsRequest.allowsCrossProject();
    }

    @Override
    public String getProjectRouting() {
        return fieldCapsRequest.getProjectRouting();
    }

    @Override
    public void setResolvedIndexExpressions(ResolvedIndexExpressions expressions) {
        fieldCapsRequest.setResolvedIndexExpressions(expressions);
    }

    @Override
    public ResolvedIndexExpressions getResolvedIndexExpressions() {
        return fieldCapsRequest.getResolvedIndexExpressions();
    }

    @Override
    public void setResolvedTargetProjects(TargetProjects targetProjects) {
        fieldCapsRequest.setResolvedTargetProjects(targetProjects);
    }

    @Override
    public TargetProjects getResolvedTargetProjects() {
        return fieldCapsRequest.getResolvedTargetProjects();
    }

    @Override
    public ActionRequestValidationException validate() {
        return fieldCapsRequest.validate();
    }

    @Override
    public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
        return fieldCapsRequest.createTask(id, type, action, parentTaskId, headers);
    }
}

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
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;

import java.io.IOException;
import java.util.Map;

public final class SearchShardsRequest extends ActionRequest implements IndicesRequest.Replaceable {
    private final SearchRequest searchRequest;

    public SearchShardsRequest(SearchRequest searchRequest) {
        this.searchRequest = searchRequest;
    }

    public SearchShardsRequest(StreamInput in) throws IOException {
        super(in);
        this.searchRequest = new SearchRequest(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        searchRequest.writeTo(out);
    }

    @Override
    public ActionRequestValidationException validate() {
        return searchRequest.validate();
    }

    @Override
    public String[] indices() {
        return searchRequest.indices();
    }

    @Override
    public IndicesOptions indicesOptions() {
        return searchRequest.indicesOptions();
    }

    @Override
    public IndicesRequest indices(String... indices) {
        searchRequest.indices(indices);
        return this;
    }

    @Override
    public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
        return searchRequest.createTask(id, type, action, parentTaskId, headers);
    }

    public SearchRequest getSearchRequest() {
        return searchRequest;
    }
}

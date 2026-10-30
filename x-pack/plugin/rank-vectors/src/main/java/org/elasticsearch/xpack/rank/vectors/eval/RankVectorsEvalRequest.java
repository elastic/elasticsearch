/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.rank.vectors.eval;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.UntypedActionRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;

/** Transport request for {@code _rank_vectors_eval}. */
public final class RankVectorsEvalRequest extends UntypedActionRequest implements IndicesRequest.Replaceable {

    private final RankVectorsEvalSpec spec;
    private String[] indices;
    private IndicesOptions indicesOptions = SearchRequest.DEFAULT_INDICES_OPTIONS;
    private boolean shouldStoreResult;

    public RankVectorsEvalRequest(RankVectorsEvalSpec spec, String[] indices) {
        this.spec = Objects.requireNonNull(spec);
        indices(indices);
    }

    public RankVectorsEvalRequest(StreamInput in) throws IOException {
        super(in);
        spec = new RankVectorsEvalSpec(in);
        indices = in.readStringArray();
        indicesOptions = IndicesOptions.readIndicesOptions(in);
    }

    public RankVectorsEvalSpec spec() {
        return spec;
    }

    @Override
    public ActionRequestValidationException validate() {
        if (indices.length > 0) {
            return null;
        }
        ActionRequestValidationException exception = new ActionRequestValidationException();
        exception.addValidationError("at least one index must be specified");
        return exception;
    }

    @Override
    public String[] indices() {
        return indices;
    }

    @Override
    public RankVectorsEvalRequest indices(String... indices) {
        this.indices = Objects.requireNonNull(indices);
        return this;
    }

    @Override
    public IndicesOptions indicesOptions() {
        return indicesOptions;
    }

    public void indicesOptions(IndicesOptions indicesOptions) {
        this.indicesOptions = Objects.requireNonNull(indicesOptions);
    }

    public void setShouldStoreResult(boolean shouldStoreResult) {
        this.shouldStoreResult = shouldStoreResult;
    }

    @Override
    public boolean getShouldStoreResult() {
        return shouldStoreResult;
    }

    @Override
    public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
        return new CancellableTask(id, type, action, "indices" + Arrays.toString(indices), parentTaskId, headers) {
            @Override
            public boolean shouldCancelChildrenOnCancellation() {
                return true;
            }
        };
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        spec.writeTo(out);
        out.writeStringArray(indices);
        indicesOptions.writeIndicesOptions(out);
    }
}

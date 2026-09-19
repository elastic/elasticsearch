/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.knneval;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.UntypedActionRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;

/** Request to estimate the recall of approximate kNN configurations against a more thorough one over the same field. */
final class KnnEvalRequest extends UntypedActionRequest implements IndicesRequest.Replaceable {

    private KnnEvalSpec knnEvalSpec;

    private IndicesOptions indicesOptions = SearchRequest.DEFAULT_INDICES_OPTIONS;
    private String[] indices = Strings.EMPTY_ARRAY;
    private boolean shouldStoreResult;

    KnnEvalRequest(KnnEvalSpec knnEvalSpec, String[] indices) {
        this.knnEvalSpec = Objects.requireNonNull(knnEvalSpec, "knn evaluation specification must not be null");
        indices(indices);
    }

    KnnEvalRequest(StreamInput in) throws IOException {
        super(in);
        knnEvalSpec = new KnnEvalSpec(in);
        indices = in.readStringArray();
        indicesOptions = IndicesOptions.readIndicesOptions(in);
    }

    KnnEvalRequest() {}

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException e = null;
        if (knnEvalSpec == null) {
            e = new ActionRequestValidationException();
            e.addValidationError("missing knn evaluation specification");
        }
        if (indices.length == 0) {
            e = e == null ? new ActionRequestValidationException() : e;
            e.addValidationError("at least one index must be specified");
        }
        return e;
    }

    public KnnEvalSpec getKnnEvalSpec() {
        return knnEvalSpec;
    }

    public void setKnnEvalSpec(KnnEvalSpec knnEvalSpec) {
        this.knnEvalSpec = knnEvalSpec;
    }

    @Override
    public KnnEvalRequest indices(String... indices) {
        Objects.requireNonNull(indices, "indices must not be null");
        for (String index : indices) {
            Objects.requireNonNull(index, "index must not be null");
        }
        this.indices = indices;
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

    public void indicesOptions(IndicesOptions indicesOptions) {
        this.indicesOptions = Objects.requireNonNull(indicesOptions, "indicesOptions must not be null");
    }

    public KnnEvalRequest setShouldStoreResult(boolean shouldStoreResult) {
        this.shouldStoreResult = shouldStoreResult;
        return this;
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
        knnEvalSpec.writeTo(out);
        out.writeStringArray(indices);
        indicesOptions.writeIndicesOptions(out);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        KnnEvalRequest that = (KnnEvalRequest) o;
        return Objects.equals(indicesOptions, that.indicesOptions)
            && Arrays.equals(indices, that.indices)
            && Objects.equals(knnEvalSpec, that.knnEvalSpec);
    }

    @Override
    public int hashCode() {
        return Objects.hash(indicesOptions, Arrays.hashCode(indices), knnEvalSpec);
    }
}

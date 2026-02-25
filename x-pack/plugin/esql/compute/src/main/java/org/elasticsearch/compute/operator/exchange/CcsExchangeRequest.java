/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.exchange;

import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.transport.AbstractTransportRequest;

import java.io.IOException;
import java.util.Map;

/**
 * An exchange request for CCS that implements {@link IndicesRequest.Replaceable} so that the
 * security layer can enforce index-level privileges for the end user on the remote cluster.
 * This wraps the same fields as {@link ExchangeRequest} but adds indices context.
 * <p>
 * The security layer rewrites {@link #indices()} via {@link Replaceable#indices(String...)} during
 * authorization (wildcard expansion, alias resolution, etc.). To preserve the original index
 * expressions for server-side validation against the exchange sink, they are stored separately
 * in {@link #originalQueryIndices()} — analogous to how {@code ClusterComputeRequest} embeds
 * original indices inside its serialized plan payload.
 */
public final class CcsExchangeRequest extends AbstractTransportRequest implements IndicesRequest.Replaceable {
    private final String exchangeId;
    private final boolean sourcesFinished;
    private String[] indices;
    private final String[] originalQueryIndices;
    private final IndicesOptions indicesOptions;

    public CcsExchangeRequest(String exchangeId, boolean sourcesFinished, String[] indices, IndicesOptions indicesOptions) {
        this.exchangeId = exchangeId;
        this.sourcesFinished = sourcesFinished;
        this.indices = indices;
        this.originalQueryIndices = indices.clone();
        this.indicesOptions = indicesOptions;
    }

    public CcsExchangeRequest(StreamInput in) throws IOException {
        super(in);
        this.exchangeId = in.readString();
        this.sourcesFinished = in.readBoolean();
        this.indices = in.readStringArray();
        this.originalQueryIndices = in.readStringArray();
        this.indicesOptions = IndicesOptions.readIndicesOptions(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(exchangeId);
        out.writeBoolean(sourcesFinished);
        out.writeStringArray(indices);
        out.writeStringArray(originalQueryIndices);
        indicesOptions.writeIndicesOptions(out);
    }

    @Override
    public TaskId getParentTask() {
        if (sourcesFinished) {
            return TaskId.EMPTY_TASK_ID;
        }
        return super.getParentTask();
    }

    public boolean sourcesFinished() {
        return sourcesFinished;
    }

    public String exchangeId() {
        return exchangeId;
    }

    /**
     * Returns the original index expressions as supplied by the querying cluster, before any
     * security rewriting via {@link Replaceable#indices(String...)}. Used for server-side
     * validation that the exchange sink was opened for these indices.
     */
    public String[] originalQueryIndices() {
        return originalQueryIndices;
    }

    @Override
    public String[] indices() {
        return indices;
    }

    @Override
    public IndicesRequest indices(String... indices) {
        this.indices = indices;
        return this;
    }

    @Override
    public IndicesOptions indicesOptions() {
        return indicesOptions;
    }

    @Override
    public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
        if (sourcesFinished == false && parentTaskId.isSet() == false) {
            assert false : "CcsExchangeRequest with sourcesFinished=false must have a parent task";
            throw new IllegalStateException("CcsExchangeRequest with sourcesFinished=false must have a parent task");
        }
        return new CancellableTask(id, type, action, "", parentTaskId, headers) {
            @Override
            public String getDescription() {
                return "ccs exchange request id=" + exchangeId;
            }
        };
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.exchange;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.transport.AbstractTransportRequest;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

public final class ExchangeRequest extends AbstractTransportRequest {
    private final String exchangeId;
    private final boolean sourcesFinished;

    public ExchangeRequest(String exchangeId, boolean sourcesFinished) {
        this.exchangeId = exchangeId;
        this.sourcesFinished = sourcesFinished;
    }

    public ExchangeRequest(StreamInput in) throws IOException {
        super(in);
        this.exchangeId = in.readString();
        this.sourcesFinished = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(exchangeId);
        out.writeBoolean(sourcesFinished);
    }

    @Override
    public TaskId getParentTask() {
        // Exchange requests with `sourcesFinished=true` complete the remote sink and return without blocking.
        // Masking the parent task allows these requests to bypass task cancellation, ensuring cleanup of the remote sink.
        // TODO: Maybe add a separate action/request for closing exchange sinks?
        if (sourcesFinished) {
            return TaskId.EMPTY_TASK_ID;
        }
        return super.getParentTask();
    }

    /**
     * True if the {@link ExchangeSourceHandler} has enough input.
     * The corresponding {@link ExchangeSinkHandler} can drain pages and finish itself.
     */
    public boolean sourcesFinished() {
        return sourcesFinished;
    }

    /**
     * Returns the exchange ID. We don't use the parent task id because it can be overwritten by a proxy node.
     */
    public String exchangeId() {
        return exchangeId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ExchangeRequest that = (ExchangeRequest) o;
        return sourcesFinished == that.sourcesFinished && exchangeId.equals(that.exchangeId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(exchangeId, sourcesFinished);
    }

    @Override
    public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
        if (sourcesFinished == false && parentTaskId.isSet() == false) {
            assert false : "ExchangeRequest with sourcesFinished=false must have a parent task";
            throw new IllegalStateException("ExchangeRequest with sourcesFinished=false must have a parent task");
        }
        return new CancellableTask(id, type, action, "", parentTaskId, headers) {
            @Override
            public String getDescription() {
                return "exchange request id=" + exchangeId;
            }
        };
    }
}

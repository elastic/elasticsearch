/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.action;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.LegacyActionRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.core.inference.InferenceContext;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

/**
 * Base class for inference action requests. Tracks request routing state to prevent potential routing loops
 * and supports both streaming and non-streaming inference operations.
 */
public abstract class BaseInferenceActionRequest extends LegacyActionRequest {

    private static final TransportVersion INFERENCE_CONTEXT = TransportVersion.fromName("inference_context");
    static final TransportVersion INFERENCE_REQUEST_ADAPTIVE_RATE_LIMITING_REMOVED = TransportVersion.fromName(
        "inference_request_adaptive_rate_limiting_removed"
    );

    public static final TransportVersion INFERENCE_REQUEST_PER_TASK_TIMEOUT_ADDED = TransportVersion.fromName(
        "inference_request_per_task_timeout_added"
    );
    /**
     * Used as a non-null marker for cases where the timeout has not been determined yet. Safe to use in this way because a timeout of 0
     * would otherwise lead to requests instantly timing out, which is not a valid use case
     */
    public static final TimeValue TIMEOUT_NOT_DETERMINED = TimeValue.ZERO;
    /**
     * The default timeout used for all task types prior to {@link BaseInferenceActionRequest#INFERENCE_REQUEST_PER_TASK_TIMEOUT_ADDED}
     */
    public static final TimeValue OLD_DEFAULT_TIMEOUT = TimeValue.THIRTY_SECONDS;

    private static final Map<TaskType, TimeValue> DEFAULT_TIMEOUTS_MAP = Map.of(
        TaskType.TEXT_EMBEDDING,
        TimeValue.timeValueSeconds(30),
        TaskType.SPARSE_EMBEDDING,
        TimeValue.timeValueSeconds(30),
        TaskType.EMBEDDING,
        TimeValue.timeValueSeconds(30),
        TaskType.RERANK,
        TimeValue.timeValueSeconds(30),
        TaskType.COMPLETION,
        TimeValue.timeValueSeconds(120),
        TaskType.CHAT_COMPLETION,
        TimeValue.timeValueSeconds(120),
        TaskType.ANY,
        TIMEOUT_NOT_DETERMINED
    );

    private final InferenceContext context;

    public BaseInferenceActionRequest(InferenceContext context) {
        super();
        this.context = context;
    }

    public BaseInferenceActionRequest(StreamInput in) throws IOException {
        super(in);
        if (in.getTransportVersion().supports(INFERENCE_REQUEST_ADAPTIVE_RATE_LIMITING_REMOVED) == false) {
            in.readBoolean();
        }

        if (in.getTransportVersion().supports(INFERENCE_CONTEXT)) {
            this.context = new InferenceContext(in);
        } else {
            this.context = InferenceContext.EMPTY_INSTANCE;
        }
    }

    /**
     * Returns the default timeout for the task type if the timeout has not been explicitly set. If the timeout has been set (i.e. it is
     * not equal to {@link #TIMEOUT_NOT_DETERMINED} or {@code null}) then the unmodified timeout is returned.
     * <p>
     * Passing {@link TaskType#ANY} returns {@link #TIMEOUT_NOT_DETERMINED} so that the appropriate timeout can be resolved after parsing
     * the task type from the request body or model.
     *
     * @param taskType The task type for which the default timeout should be returned
     * @param existingTimeout The currently set timeout, which may be {@link #TIMEOUT_NOT_DETERMINED} or {@code null}
     */
    public static TimeValue resolveTimeoutForTaskType(TaskType taskType, @Nullable TimeValue existingTimeout) {
        if (existingTimeout == null || existingTimeout.equals(TIMEOUT_NOT_DETERMINED)) {
            existingTimeout = DEFAULT_TIMEOUTS_MAP.get(taskType);
        }
        return existingTimeout;
    }

    /**
     * Returns the default timeout based on the task type.
     * <p>
     * Passing {@link TaskType#ANY} returns {@link #TIMEOUT_NOT_DETERMINED} so that the appropriate timeout can be resolved later after
     * parsing the task type from the request body or model.
     *
     * @param taskType The task type for which the default timeout should be returned
     */
    public static TimeValue getDefaultTimeoutForTaskType(TaskType taskType) {
        Objects.requireNonNull(taskType);
        return DEFAULT_TIMEOUTS_MAP.get(taskType);
    }

    public abstract boolean isStreaming();

    public abstract TaskType getTaskType();

    public abstract String getInferenceEntityId();

    public InferenceContext getContext() {
        return context;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        if (out.getTransportVersion().supports(INFERENCE_REQUEST_ADAPTIVE_RATE_LIMITING_REMOVED) == false) {
            out.writeBoolean(true);
        }

        if (out.getTransportVersion().supports(INFERENCE_CONTEXT)) {
            context.writeTo(out);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BaseInferenceActionRequest that = (BaseInferenceActionRequest) o;
        return Objects.equals(context, that.context);
    }

    @Override
    public int hashCode() {
        return Objects.hash(context);
    }
}

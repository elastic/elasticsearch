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
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.core.inference.InferenceContext;

import java.io.IOException;
import java.util.Objects;

/**
 * Base class for inference action requests. Tracks request routing state to prevent potential routing loops
 * and supports both streaming and non-streaming inference operations.
 */
public abstract class BaseInferenceActionRequest extends LegacyActionRequest {

    private static final TransportVersion INFERENCE_CONTEXT = TransportVersion.fromName("inference_context");

    private boolean hasBeenRerouted;
    private final InferenceContext context;

    public BaseInferenceActionRequest(InferenceContext context) {
        super();
        this.context = context;
    }

    public BaseInferenceActionRequest(StreamInput in) throws IOException {
        super(in);
        this.hasBeenRerouted = in.readBoolean();
        if (in.getTransportVersion().supports(INFERENCE_CONTEXT)) {
            this.context = new InferenceContext(in);
        } else {
            this.context = InferenceContext.EMPTY_INSTANCE;
        }
    }

    public abstract boolean isStreaming();

    public abstract TaskType getTaskType();

    public abstract String getInferenceEntityId();

    public void setHasBeenRerouted(boolean hasBeenRerouted) {
        this.hasBeenRerouted = hasBeenRerouted;
    }

    public boolean hasBeenRerouted() {
        return hasBeenRerouted;
    }

    public InferenceContext getContext() {
        return context;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBoolean(hasBeenRerouted);
        if (out.getTransportVersion().supports(INFERENCE_CONTEXT)) {
            context.writeTo(out);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BaseInferenceActionRequest that = (BaseInferenceActionRequest) o;
        return hasBeenRerouted == that.hasBeenRerouted && Objects.equals(context, that.context);
    }

    @Override
    public int hashCode() {
        return Objects.hash(hasBeenRerouted, context);
    }
}

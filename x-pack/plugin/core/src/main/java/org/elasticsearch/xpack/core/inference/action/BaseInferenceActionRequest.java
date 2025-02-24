/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.action;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.inference.TaskType;

import java.io.IOException;

/**
 * Base class for inference action requests. Tracks request routing state to prevent potential routing loops
 * and supports both streaming and non-streaming inference operations.
 */
public abstract class BaseInferenceActionRequest extends ActionRequest {

    private boolean hasBeenRerouted;

    public BaseInferenceActionRequest() {
        super();
    }

    public BaseInferenceActionRequest(StreamInput in) throws IOException {
        super(in);
        if (in.getTransportVersion().onOrAfter(TransportVersions.INFERENCE_REQUEST_ADAPTIVE_RATE_LIMITING)) {
            this.hasBeenRerouted = in.readBoolean();
        } else {
            // For backwards compatibility, we treat all inference requests coming from ES nodes having
            // a version pre-node-local-rate-limiting as already rerouted to maintain pre-node-local-rate-limiting behavior.
            this.hasBeenRerouted = true;
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

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        if (out.getTransportVersion().onOrAfter(TransportVersions.INFERENCE_REQUEST_ADAPTIVE_RATE_LIMITING)) {
            out.writeBoolean(hasBeenRerouted);
        }
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.queries;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.inference.MinimalServiceSettings;

import java.io.IOException;
import java.util.Objects;

public class InferenceEndpointKey implements Writeable {
    private final String inferenceId;
    private final MinimalServiceSettings serviceSettings;

    public InferenceEndpointKey(String inferenceId, MinimalServiceSettings serviceSettings) {
        this.inferenceId = inferenceId;
        this.serviceSettings = serviceSettings;
    }

    public InferenceEndpointKey(StreamInput in) throws IOException {
        this.inferenceId = in.readString();
        this.serviceSettings = in.readNamedWriteable(MinimalServiceSettings.class);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(inferenceId);
        out.writeNamedWriteable(serviceSettings);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        InferenceEndpointKey that = (InferenceEndpointKey) o;
        return Objects.equals(inferenceId, that.inferenceId) && Objects.equals(serviceSettings, that.serviceSettings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(inferenceId, serviceSettings);
    }
}

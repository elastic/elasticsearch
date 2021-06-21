/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.deployment;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;
import java.util.Locale;

public enum TrainedModelDeploymentState implements Writeable {

    STARTING, STARTED, STOPPING, STOPPED;

    public static TrainedModelDeploymentState fromString(String name) {
        return valueOf(name.trim().toUpperCase(Locale.ROOT));
    }

    public static TrainedModelDeploymentState fromStream(StreamInput in) throws IOException {
        return in.readEnum(TrainedModelDeploymentState.class);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeEnum(this);
    }

    @Override
    public String toString() {
        return name().toLowerCase(Locale.ROOT);
    }
}

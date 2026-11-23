/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;

/**
 * Measured search demand of one shard copy on one node. {@code cpuDemand} is vCPU and {@code threadDemand} is threads.
 */
public record SearchCopyDemand(double cpuDemand, double threadDemand) implements Writeable {

    public SearchCopyDemand {
        assert Double.isFinite(cpuDemand) && cpuDemand >= 0d : "cpuDemand must be finite and non-negative: " + cpuDemand;
        assert Double.isFinite(threadDemand) && threadDemand >= 0d : "threadDemand must be finite and non-negative: " + threadDemand;
    }

    public SearchCopyDemand(StreamInput in) throws IOException {
        this(in.readDouble(), in.readDouble());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeDouble(cpuDemand);
        out.writeDouble(threadDemand);
    }
}

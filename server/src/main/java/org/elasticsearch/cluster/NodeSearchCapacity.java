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
 * Search capacity of one node. {@code nodeCompute} is the pod's requested CPU in vCPU and {@code threadBudget} is the pod's planned
 * concurrent task budget in threads. A node with no entry has unknown capacity.
 */
public record NodeSearchCapacity(double nodeCompute, double threadBudget) implements Writeable {

    public NodeSearchCapacity {
        assert Double.isFinite(nodeCompute) && nodeCompute >= 0d : "nodeCompute must be finite and non-negative: " + nodeCompute;
        assert Double.isFinite(threadBudget) && threadBudget >= 0d : "threadBudget must be finite and non-negative: " + threadBudget;
    }

    public NodeSearchCapacity(StreamInput in) throws IOException {
        this(in.readDouble(), in.readDouble());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeDouble(nodeCompute);
        out.writeDouble(threadBudget);
    }
}

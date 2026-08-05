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
 * The estimated heap in use by a node
 *
 * @param totalHeapUsage The total estimated heap usage, including things like index metadata, hosted shards, indexing infrastructure, etc.
 * @param hostedShardsHeapUsage The estimated heap usage attributable to hosted shards only
 */
public record NodeHeapEstimates(long totalHeapUsage, long hostedShardsHeapUsage) implements Writeable {

    public NodeHeapEstimates {
        assert totalHeapUsage >= 0;
        assert hostedShardsHeapUsage >= 0;
    }

    public NodeHeapEstimates(StreamInput in) throws IOException {
        this(in.readVLong(), in.readVLong());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(totalHeapUsage);
        out.writeVLong(hostedShardsHeapUsage);
    }
}

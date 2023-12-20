/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.health.node;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.health.HealthStatus;

import java.io.IOException;

/**
 * The health status of the disk space of this node along with the cause.
 */
public record DiskHealthInfo(HealthStatus healthStatus, @Nullable Cause cause) implements Writeable {
    DiskHealthInfo(HealthStatus healthStatus) {
        this(healthStatus, null);
    }

    public DiskHealthInfo(StreamInput in) throws IOException {
        this(in.readEnum(HealthStatus.class), in.readOptionalEnum(Cause.class));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        healthStatus.writeTo(out);
        out.writeOptionalEnum(cause);
    }

    public enum Cause {
        NODE_OVER_HIGH_THRESHOLD,
        NODE_OVER_THE_FLOOD_STAGE_THRESHOLD,
        FROZEN_NODE_OVER_FLOOD_STAGE_THRESHOLD,
        NODE_HAS_NO_DISK_STATS
    }
}

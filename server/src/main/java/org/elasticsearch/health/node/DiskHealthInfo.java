/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.health.node;

import org.elasticsearch.health.HealthStatus;

/**
 * The health status of the disk space of this node along with the cause.
 */
record DiskHealthInfo(HealthStatus healthStatus, Cause cause) {
    DiskHealthInfo(HealthStatus healthStatus) {
        this(healthStatus, null);
    }

    enum Cause {
        NODE_OVER_HIGH_THRESHOLD,
        NODE_OVER_THE_FLOOD_STAGE_THRESHOLD,
        FROZEN_NODE_OVER_FLOOD_STAGE_THRESHOLD,
        NODE_HAS_NO_DISK_STATS
    }
}

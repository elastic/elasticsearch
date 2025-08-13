/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.common;

import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.inference.TaskType;

public class NoopNodeLocalRateLimitCalculator implements InferenceServiceRateLimitCalculator {

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        // Do nothing
    }

    public boolean isTaskTypeReroutingSupported(String serviceName, TaskType taskType) {
        return false;
    }

    public RateLimitAssignment getRateLimitAssignment(String service, TaskType taskType) {
        return null;
    }
}

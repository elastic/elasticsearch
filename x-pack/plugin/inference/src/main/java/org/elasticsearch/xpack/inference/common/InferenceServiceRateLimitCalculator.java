/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.common;

import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.inference.TaskType;

public interface InferenceServiceRateLimitCalculator extends ClusterStateListener {

    boolean isTaskTypeReroutingSupported(String serviceName, TaskType taskType);

    RateLimitAssignment getRateLimitAssignment(String service, TaskType taskType);
}

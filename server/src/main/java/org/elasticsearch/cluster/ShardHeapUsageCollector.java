/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster;

import org.elasticsearch.action.ActionListener;

import java.util.Map;

public interface ShardHeapUsageCollector {

    /**
     * This will be used when there are no heap usage suppliers available
     */
    ShardHeapUsageCollector EMPTY = listener -> listener.onResponse(Map.of());

    /**
     * Collect the heap usage for every node in the cluster
     *
     * @param listener The listener which will receive the results
     */
    void collectClusterHeapUsage(ActionListener<Map<String, ShardHeapUsage>> listener);
}

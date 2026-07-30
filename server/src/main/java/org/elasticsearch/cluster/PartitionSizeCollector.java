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

/**
 * Collects the size in bytes of a node partition for use in {@link ClusterInfo}.
 */
public interface PartitionSizeCollector {

    /**
     * Used when no partition size collector is available.
     */
    PartitionSizeCollector EMPTY = listener -> listener.onResponse(Map.of());

    /**
     * Collects the partition size in bytes, keyed by node ID.
     *
     * @param listener The listener which will receive a map of node ID to partition size in bytes.
     */
    void collectPartitionSizes(ActionListener<Map<String, Long>> listener);
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.core.Releasable;

public interface GroupingAggregatorState extends Releasable {
    void enableGroupIdTracking(SeenGroupIds seenGroupIds);

    /**
     * Optionally pre-size aggregation state to handle incoming groups up to {@code maxPossibleGroupId}.
     * Implement this to avoid resizing for each input row.
     */
    default void presizeGroupingStates(int maxPossibleGroupId) {}
}

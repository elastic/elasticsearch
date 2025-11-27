/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.health;

import org.elasticsearch.health.node.tracker.HealthTracker;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

public class SimpleHealthTrackerRegistry {

    private final Set<HealthTracker<?>> trackers = new HashSet<>();
    private final Set<HealthIndicatorService> healthIndicatorServices = new HashSet<>();

    public void register(HealthTracker<?> testTracker, HealthIndicatorService testIndicator) {
        trackers.add(testTracker);
        healthIndicatorServices.add(testIndicator);
    }

    public List<HealthTracker<?>> addTrackers(List<HealthTracker<?>> trackerList) {
        return Stream.concat(trackers.stream(), trackerList.stream()).toList();
    }

    public Stream<HealthIndicatorService> mergeHealthIndicatorServices(Stream<HealthIndicatorService> otherIndicators) {
        return Stream.concat(otherIndicators, healthIndicatorServices.stream());
    }
}

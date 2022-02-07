/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.health;

import java.util.List;
import java.util.TreeMap;

import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;

/**
 * This service collects health indicators from all modules and plugins of elasticsearch
 */
public class HealthService {

    private final List<HealthIndicatorService> healthIndicatorServices;

    public HealthService(List<HealthIndicatorService> healthIndicatorServices) {
        this.healthIndicatorServices = healthIndicatorServices;
    }

    public List<HealthComponentResult> getHealth() {
        return List.copyOf(
            healthIndicatorServices.stream()
                .map(HealthIndicatorService::calculate)
                .collect(
                    groupingBy(
                        HealthIndicatorResult::component,
                        TreeMap::new,
                        collectingAndThen(toList(), HealthService::createComponentFromIndicators)
                    )
                )
                .values()
        );
    }

    private static HealthComponentResult createComponentFromIndicators(List<HealthIndicatorResult> indicators) {
        assert indicators.size() > 0 : "Component should not be non empty";
        assert indicators.stream().map(HealthIndicatorResult::component).distinct().count() == 1L
            : "Should not mix indicators from different components";
        return new HealthComponentResult(
            indicators.get(0).component(),
            HealthStatus.merge(indicators.stream().map(HealthIndicatorResult::status)),
            indicators
        );
    }
}

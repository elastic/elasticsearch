/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.health;

import java.util.Collection;
import java.util.List;

/**
 * This service collects health indicators from all modules and plugins of elasticsearch
 */
public class HealthService {

    private final List<HealthIndicatorService> healthIndicatorServices;

    public HealthService(List<HealthIndicatorService> healthIndicatorServices) {
        this.healthIndicatorServices = healthIndicatorServices;
    }

    public Collection<HealthIndicatorResult> getHealthIndicators() {
        return healthIndicatorServices.stream().map(HealthIndicatorService::calculate).toList();
    }
}

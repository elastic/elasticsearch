/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.health;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HealthService {

    private final List<HealthIndicatorService> healthIndicatorServices;

    public HealthService(List<HealthIndicatorService> healthIndicatorServices) {
        this.healthIndicatorServices = healthIndicatorServices;
    }

    public Map<String, List<HealthIndicator>> getHealthIndicatorss() {
        final Map<String, List<HealthIndicator>> map = new HashMap<>();
        for (HealthIndicatorService healthIndicatorService : healthIndicatorServices) {
            for (HealthIndicator healthIndicator : healthIndicatorService.getIndicators()) {
                map.computeIfAbsent(healthIndicator.getComponent(), s -> new ArrayList<>()).add(healthIndicator);
            }
        }
        return Collections.unmodifiableMap(map);
    }
}

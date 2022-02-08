/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.health;

/**
 * This is a service interface used to report health indicators from the different plugins.
 */
public interface HealthIndicatorService {

    String name();

    String component();

    HealthIndicatorResult calculate();

    default HealthIndicatorResult createIndicator(HealthStatus status, String summary, HealthIndicatorDetails details) {
        return new HealthIndicatorResult(name(), component(), status, summary, details);
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.health.node.check;

import org.elasticsearch.health.node.LocalHealthMonitor;
import org.elasticsearch.health.node.UpdateHealthInfoCacheAction;

/**
 * Interface for health checks that will be executed by the {@link LocalHealthMonitor}.
 *
 * @param <T> the type that the health check will return.
 */
public interface HealthCheck<T> {
    /**
     * Determine the health info for this health check.
     *
     * @return the health info.
     */
    T getHealth();

    /**
     * Add the health info to the request builder.
     *
     * @param builder the builder to add the health info to.
     * @param healthInfo the health info to add.
     */
    void addHealthToBuilder(UpdateHealthInfoCacheAction.Request.Builder builder, T healthInfo);
}

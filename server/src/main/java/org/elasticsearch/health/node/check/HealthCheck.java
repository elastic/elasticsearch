/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.health.node.check;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.health.node.LocalHealthMonitor;
import org.elasticsearch.health.node.UpdateHealthInfoCacheAction;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Base class for health checks that will be executed by the {@link LocalHealthMonitor}. The base class takes care of maintaining
 * a reference to the previous value of the health check, to ensure health updates are only sent once the value has actually changed.
 *
 * @param <T> the type that the health check will return.
 */
public abstract class HealthCheck<T> {
    private static final Logger logger = LogManager.getLogger(HealthCheck.class);

    // Keeps the latest health state that was successfully reported to the current health node.
    private final AtomicReference<T> reference = new AtomicReference<>();

    /**
     * Determine the health info for this health check.
     *
     * @return the health info.
     */
    public abstract T getHealth();

    /**
     * Add the health info to the request builder.
     *
     * @param builder the builder to add the health info to.
     * @param healthInfo the health info to add.
     */
    public abstract void setBuilder(UpdateHealthInfoCacheAction.Request.Builder builder, T healthInfo);

    /**
     * Perform the health check by determining the current health and comparing that to the previous health.
     * If the health has changed, we add the new health info to the request builder and return a callback that should be executed after
     * the update request has been executed successfully. The reason for using callbacks, is that we want to unsure we use the same
     * object references as when we performed the health check.
     *
     * @param builder the request builder to append to health info to.
     * @return null when the health info for this health check hasn't changed. Otherwise, a callback that updates the reference to the
     * latest health info with the health info determined in this check.
     */
    public Runnable performHealthCheck(UpdateHealthInfoCacheAction.Request.Builder builder) {
        var previousHealth = reference.get();
        var currentHealth = getHealth();
        if (currentHealth.equals(previousHealth)) {
            return null;
        }

        setBuilder(builder, currentHealth);
        return () -> {
            // Update the last reported value only if the health node hasn't changed.
            if (reference.compareAndSet(previousHealth, currentHealth)) {
                logger.debug("Health info [{}] successfully sent, last reported value: {}.", currentHealth, reference.get());
            }
        };
    }

    /**
     * Reset the latest health info reference to null. Should be used when, for example, the master or health node has changed.
     */
    public void reset() {
        reference.set(null);
    }

    public T getReferenceValue() {
        return reference.get();
    }
}

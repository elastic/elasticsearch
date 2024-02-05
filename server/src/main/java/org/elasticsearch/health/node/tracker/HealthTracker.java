/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.health.node.tracker;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.health.node.LocalHealthMonitor;
import org.elasticsearch.health.node.UpdateHealthInfoCacheAction;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Base class for health trackers that will be executed by the {@link LocalHealthMonitor}. It keeps track of the last
 * reported value and can retrieve the current health status when requested.
 *
 * @param <T> the type of the health check result they track
 */
public abstract class HealthTracker<T> {
    private static final Logger logger = LogManager.getLogger(HealthTracker.class);

    private final AtomicReference<T> lastReportedValue = new AtomicReference<>();

    /**
     * Determine the health info for this health check.
     *
     * @return the health info.
     */
    public abstract T checkCurrentHealth();

    /**
     * Add the health info to the request builder.
     *
     * @param builder the builder to add the health info to.
     * @param healthInfo the health info to add.
     */
    public abstract void addToRequestBuilder(UpdateHealthInfoCacheAction.Request.Builder builder, T healthInfo);

    /**
     * Create a new {@link HealthProgress} instance by getting the current last reported value and determining the health info at this time.
     *
     * @return the new {@link HealthProgress} instance.
     */
    public HealthProgress<T> trackHealth() {
        return new HealthProgress<>(this, lastReportedValue.get(), checkCurrentHealth());
    }

    /**
     * Update the last reported health info to <code>current</code>, but only when the value inside <code>lastReportedValue</code>
     * is equal to <code>previous</code>.
     *
     * @param previous the previous value that should be in <code>lastReportedValue</code> at the time of execution.
     * @param current the value that should be stored in <code>lastReportedValue</code>.
     */
    public void updateLastReportedHealth(T previous, T current) {
        if (lastReportedValue.compareAndSet(previous, current)) {
            logger.debug("Health info [{}] successfully sent, last reported value: {}.", current, previous);
        }
    }

    /**
     * Reset the value of <code>lastReportedValue</code> to <code>null</code>.
     * Should be used when, for example, the master or health node has changed.
     */
    public void reset() {
        lastReportedValue.set(null);
    }

    public T getLastReportedValue() {
        return lastReportedValue.get();
    }

    /**
     * A record for storing the previous and current value of a health check. This allows us to be sure no concurrent processes have
     * updated the health check's reference value.
     *
     * @param <T> the type that the health tracker returns
     */
    public record HealthProgress<T>(HealthTracker<T> healthTracker, T previousHealth, T currentHealth) {
        public boolean hasChanged() {
            return Objects.equals(previousHealth, currentHealth) == false;
        }

        /**
         * See {@link HealthTracker#addToRequestBuilder}.
         */
        public void updateRequestBuilder(UpdateHealthInfoCacheAction.Request.Builder builder) {
            healthTracker.addToRequestBuilder(builder, currentHealth);
        }

        /**
         * Update the reference value of the health tracker with the current health info.
         * See {@link HealthTracker#updateLastReportedHealth} for more info.
         */
        public void recordProgressIfRelevant() {
            healthTracker.updateLastReportedHealth(previousHealth, currentHealth);
        }
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.health.node.tracker;

import org.elasticsearch.health.node.LocalHealthMonitor;
import org.elasticsearch.health.node.UpdateHealthInfoCacheAction;

/**
 * Base class for health trackers that will be executed by the {@link LocalHealthMonitor}. It keeps track of the last
 * reported health and can retrieve the current health status when requested.
 *
 * @param <T> the type of the health check result they track
 */
public abstract class HealthTracker<T> {

    /**
     * We can "simply" use a volatile field here, as we've ensured only one monitoring instance/thread at a time can update this value.
     */
    private volatile T lastDeterminedHealth;

    /**
     * Determine the health info for this health check.
     *
     * @return the health info.
     */
    protected abstract T determineCurrentHealth();

    /**
     * Add the health info to the request builder.
     *
     * @param builder the builder to add the health info to.
     * @param healthInfo the health info to add.
     */
    protected abstract void addToRequestBuilder(UpdateHealthInfoCacheAction.Request.Builder builder, T healthInfo);

    /**
     * Add the last reported health to the request builder.
     */
    public void addToRequestBuilder(UpdateHealthInfoCacheAction.Request.Builder builder) {
        addToRequestBuilder(builder, lastDeterminedHealth);
    }

    /**
     * Determine the current health info for this tracker and check if it has changed from the last reported value. When the health has
     * changed, we'll store the new health as the last reported value.
     *
     * @return whether the health has changed.
     */
    public boolean checkHealthChanged() {
        var health = determineCurrentHealth();
        assert health != null : "health trackers must return unknown health instead of null";
        if (health.equals(lastDeterminedHealth)) {
            return false;
        }
        lastDeterminedHealth = health;
        return true;
    }

    /**
     * Reset the value of <code>lastReportedValue</code> to <code>null</code>.
     * Should be used when, for example, the master or health node has changed.
     */
    public void reset() {
        lastDeterminedHealth = null;
    }

    public T getLastDeterminedHealth() {
        return lastDeterminedHealth;
    }
}

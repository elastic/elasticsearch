/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.iplocation.api;

/**
 * Listener for database first-time availability events.
 * Fired when a database becomes available on a node for the first time,
 * or becomes available again after being removed. Not fired on database
 * updates (when a newer version replaces an existing one).
 */
@FunctionalInterface
public interface DatabaseAvailabilityListener {

    /**
     * Called when a database becomes available for the first time on this node.
     *
     * @param projectId    the project id string
     * @param databaseFile the database file name (e.g., "GeoLite2-City.mmdb")
     */
    void onDatabaseAvailable(String projectId, String databaseFile);
}

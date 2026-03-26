/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.iplocation.api;

import org.elasticsearch.core.Nullable;

import java.util.List;

/**
 * Service API for IP data lookups and database lifecycle management.
 * Separates three orthogonal concerns:
 * <ol>
 *   <li>Lookup: create live handles to databases ({@link #createIpDataLookup})</li>
 *   <li>Metadata: get field info without requiring database download ({@link #getIpDataLookupInfo})</li>
 *   <li>Lifecycle: notification and download management ({@link #addDatabaseAvailabilityListener},
 *       {@link #requestDownloads}, {@link #cancelDownloadRequest})</li>
 * </ol>
 */
public interface IpLocationService {

    /**
     * Create a live lookup handle for a specific database and properties.
     * Returns null if the database is not currently available on this node.
     * <p>
     * The returned {@link IpDataLookup} is a "live handle": each {@code lookup()} call
     * internally obtains the latest database from the databases map using
     * reference counting, so the instance remains valid across database
     * updates and re-downloads without replacement.
     *
     * @param projectId     the project id string
     * @param databaseFile  the database file name (e.g. "GeoLite2-City.mmdb")
     * @param propertyNames the properties to extract (e.g. ["city_name", "country_name"]),
     *                      or null for all default properties
     * @return a live IpDataLookup handle, or null if the database is not available
     * @throws IllegalArgumentException if database type or properties are invalid
     */
    @Nullable
    IpDataLookup createIpDataLookup(String projectId, String databaseFile, List<String> propertyNames);

    /**
     * Get metadata for a database. Always available immediately, derived from
     * file name convention -- no download needed.
     * Returns all fields for the database type (not filtered by properties).
     *
     * @param databaseFile the database file name (e.g. "GeoLite2-City.mmdb")
     * @return metadata with ordered field map and database type string, or null if unknown
     * @throws IllegalArgumentException if the database type cannot be determined
     */
    @Nullable
    IpDataLookupInfo getIpDataLookupInfo(String databaseFile);

    /**
     * Register a listener for database first-time availability events on this node.
     * The listener fires when a database is loaded for the first time, but not
     * when an existing database is updated to a newer version.
     * <p>
     * Registration must happen before cluster state events begin (i.e. during
     * {@code getProcessors()} or {@code createComponents()}).
     */
    void addDatabaseAvailabilityListener(DatabaseAvailabilityListener listener);

    /**
     * Request that the service begins downloading databases for a project.
     * On the first call per project (transition from not-requested to requested),
     * triggers an immediate on-demand download run in addition to enabling
     * periodic downloads. Idempotent for subsequent calls.
     *
     * @param projectId the project id string
     */
    void requestDownloads(String projectId);

    /**
     * Cancel the download request for a project. Periodic downloads for this
     * project will stop on the next downloader run (unless eagerDownload is
     * enabled separately). Does not stop any in-progress download.
     *
     * @param projectId the project id string
     */
    void cancelDownloadRequest(String projectId);

    IpLocationService NOOP = new IpLocationService() {
        @Override
        public IpDataLookup createIpDataLookup(String p, String f, List<String> n) {
            return null;
        }

        @Override
        public IpDataLookupInfo getIpDataLookupInfo(String f) {
            return null;
        }

        @Override
        public void addDatabaseAvailabilityListener(DatabaseAvailabilityListener l) {}

        @Override
        public void requestDownloads(String p) {}

        @Override
        public void cancelDownloadRequest(String p) {}
    };
}

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
     *                      or null for all default properties. See {@link DatabaseProperty} for valid values.
     * @return a live IpDataLookup handle, or null if the database is not available
     * @throws UnsupportedDatabaseTypeException if the database type is not recognized
     * @throws IllegalArgumentException if properties are invalid for the database type
     */
    @Nullable
    IpDataLookup createIpDataLookup(String projectId, String databaseFile, List<String> propertyNames);

    /**
     * Get metadata for a database, returning all fields for the database type (not filtered by properties).
     * Available immediately without waiting for database download -- does not require a project context.
     * <p>
     * Implementations should resolve the database type consistently with {@link #createIpDataLookup}:
     * if the database is already loaded, the type should be read from the database metadata rather
     * than guessed from the filename. This ensures that both methods agree on the database type for
     * non-standard filenames (e.g. ipinfo databases). Filename-based guessing is acceptable as a
     * fallback when the database is not yet loaded.
     *
     * @param databaseFile the database file name (e.g. "GeoLite2-City.mmdb")
     * @return metadata with ordered field map and database type string, or null if unknown
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
     * Request that an {@link IpLocationConsumer} be registered as needing IP databases for the given project.
     * The implementation decides which nodes are relevant for the consumer and may trigger an immediate
     * download in addition to enabling periodic downloads.
     * <p>
     * Idempotent: calling repeatedly with the same arguments has no additional effect. Implementations
     * are not required to perform any work &mdash; {@link #NOOP} and other bridging implementations may ignore
     * the call entirely.
     *
     * @param projectId the project id string
     * @param consumer  the consumer type requesting the download
     * Implementations that back this on cluster state (e.g. master-routed state updates) are
     * asynchronous and best-effort: the call returns immediately after submitting the update request,
     * before it has been applied, and failures in applying the update are not propagated to the caller.
     * Observable side effects on ingest nodes are eventually consistent. Callers must not treat method
     * return as confirmation that registration has taken effect.
     */
    void requestDownloads(String projectId, IpLocationConsumer consumer);

    /**
     * Request that a previously {@link #requestDownloads requested} consumer be unregistered for the given
     * project. Downloads stop only when all consumers for that project have unregistered. Does not
     * interrupt an in-progress download.
     * <p>
     * Idempotent: safe to call repeatedly. {@link #NOOP} and other bridging implementations may ignore the call.
     *
     * @param projectId the project id string
     * @param consumer  the consumer type cancelling its request
     * Same async, best-effort, eventually-consistent contract as {@link #requestDownloads} for
     * implementations backed by cluster-state updates.
     */
    void cancelDownloadRequest(String projectId, IpLocationConsumer consumer);

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
        public void requestDownloads(String p, IpLocationConsumer c) {}

        @Override
        public void cancelDownloadRequest(String p, IpLocationConsumer c) {}
    };
}

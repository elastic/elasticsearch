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

import java.io.IOException;
import java.util.Map;

/**
 * Performs IP data lookups against a resolved database.
 * <p>
 * Each invocation of {@link #lookup} obtains the latest database from the
 * internal databases map, so a single instance remains valid across database
 * updates and re-downloads without replacement.
 */
public interface IpDataLookup {

    /**
     * Look up IP data and push results to the collector.
     * <p>
     * Return value semantics (three-way):
     * <ul>
     *   <li>{@code null} -- the database is currently unavailable (e.g. not yet
     *       downloaded or removed).</li>
     *   <li>{@code false} -- the IP address was not found in the database. No collector
     *       methods are called.</li>
     *   <li>{@code true} -- data was found and pushed to the collector.</li>
     * </ul>
     */
    @Nullable
    Boolean lookup(String ip, IpLocationInfoCollector collector) throws IOException;

    /**
     * Convenience method for consumers that want a simple Map result.
     * Uses {@link IpLocationInfoMapCollector} internally. Preserves three-way
     * semantics: null (unavailable), empty map (not found), non-empty map (data).
     */
    @Nullable
    default Map<String, Object> lookup(String ip) throws IOException {
        IpLocationInfoMapCollector collector = new IpLocationInfoMapCollector();
        Boolean result = lookup(ip, collector);
        if (result == null) {
            return null;
        }
        if (result == false) {
            return Map.of();
        }
        return collector;
    }

    /**
     * Whether the underlying database is still valid (not expired).
     */
    boolean isValid();

    /**
     * Returns the metadata/field info for this lookup.
     */
    IpDataLookupInfo getInfo();
}

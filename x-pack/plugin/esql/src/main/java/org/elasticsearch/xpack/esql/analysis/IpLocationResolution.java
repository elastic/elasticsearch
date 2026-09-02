/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.iplocation.api.IpDataLookupInfo;

import java.util.Map;

/**
 * Carries the IP database metadata the {@code ResolveIpLocation} analyzer rule needs to resolve the output columns of
 * {@code IP_LOCATION} commands.
 */
public interface IpLocationResolution {

    /**
     * Whether the IP location service was available when this resolution was built. When {@code false} no {@code IP_LOCATION}
     * command can be resolved and the rule leaves the node unresolved with a dedicated message.
     */
    boolean serviceAvailable();

    /**
     * The database metadata for the given database file, or {@code null} when the service is unavailable or the database file
     * is not recognized.
     */
    @Nullable
    IpDataLookupInfo databaseInfo(String databaseFile);

    /**
     * Resolution used when the IP location service is unavailable (e.g. minimal or test contexts that do not wire the service).
     */
    IpLocationResolution SERVICE_UNAVAILABLE = new IpLocationResolution() {
        @Override
        public boolean serviceAvailable() {
            return false;
        }

        @Override
        public IpDataLookupInfo databaseInfo(String databaseFile) {
            return null;
        }
    };

    /**
     * Builds a resolution backed by an already-fetched map of database metadata keyed by database file name. A missing key
     * means the corresponding database file is not recognized.
     */
    static IpLocationResolution fromPrefetched(Map<String, IpDataLookupInfo> databaseInfo) {
        return new IpLocationResolution() {
            @Override
            public boolean serviceAvailable() {
                return true;
            }

            @Override
            public IpDataLookupInfo databaseInfo(String databaseFile) {
                return databaseInfo.get(databaseFile);
            }
        };
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest.geoip;

/**
 * Provides construction and initialization logic for {@link GeoIpDatabase} instances.
 */
public interface GeoIpDatabaseProvider {
    /**
     * @param name the name of the database to provide. Default database names that should always be supported are listed in
     *             {@link IngestGeoIpPlugin#DEFAULT_DATABASE_FILENAMES}.
     * @return a ready-to-use database instance, or <code>null</code> if no database could be loaded.
     */
    GeoIpDatabase getDatabase(String name);
}

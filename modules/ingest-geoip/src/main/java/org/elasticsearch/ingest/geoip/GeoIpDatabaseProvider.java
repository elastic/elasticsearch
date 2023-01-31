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
     * Determines if the given database name corresponds to an expired database. Expired databases will not be loaded.
     * <br/><br/>
     * Verifying database expiration is left to each provider implementation to determine. A return value of <code>false</code> does not
     * preclude the possibility of a provider returning <code>true</code> in the future.
     *
     * @param name the name of the database to provide.
     * @return <code>false</code> IFF the requested database file is expired,
     *         <code>true</code> for all other cases (including unknown file name, file missing, wrong database type, etc).
     */
    Boolean isValid(String name);

    /**
     * @param name the name of the database to provide. Default database names that should always be supported are listed in
     *             {@link IngestGeoIpPlugin#DEFAULT_DATABASE_FILENAMES}.
     * @return a ready-to-use database instance, or <code>null</code> if no database could be loaded.
     */
    GeoIpDatabase getDatabase(String name);
}

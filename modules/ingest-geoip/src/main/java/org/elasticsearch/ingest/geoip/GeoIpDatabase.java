/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest.geoip;

import com.maxmind.geoip2.model.AsnResponse;
import com.maxmind.geoip2.model.CityResponse;
import com.maxmind.geoip2.model.CountryResponse;

import org.elasticsearch.core.Nullable;

import java.io.IOException;
import java.net.InetAddress;

/**
 * Provides a uniform interface for interacting with various GeoIP databases.
 */
public interface GeoIpDatabase {

    /**
     * @return the database type as it is detailed in the database file metadata
     * @throws IOException if the database file could not be read or the data could not be accessed
     */
    String getDatabaseType() throws IOException;

    /**
     * @param ipAddress the IP address to look up
     * @return a response containing the city data for the given address if it exists, or <code>null</code> if it could not be found
     * @throws UnsupportedOperationException may be thrown if the implementation does not support retrieving city data
     */
    @Nullable
    CityResponse getCity(InetAddress ipAddress);

    /**
     * @param ipAddress the IP address to look up
     * @return a response containing the country data for the given address if it exists, or <code>null</code> if it could not be found
     * @throws UnsupportedOperationException may be thrown if the implementation does not support retrieving country data
     */
    @Nullable
    CountryResponse getCountry(InetAddress ipAddress);

    /**
     * @param ipAddress the IP address to look up
     * @return a response containing the Autonomous System Number for the given address if it exists, or <code>null</code> if it could not
     *         be found
     * @throws UnsupportedOperationException may be thrown if the implementation does not support retrieving ASN data
     */
    @Nullable
    AsnResponse getAsn(InetAddress ipAddress);

    /**
     * Releases the current database object. Called after processing a single document. Databases should be closed or returned to a
     * resource pool. No further interactions should be expected.
     * @throws IOException if the implementation encounters any problem while cleaning up
     */
    void release() throws IOException;
}

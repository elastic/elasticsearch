/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.geoip;

import com.maxmind.geoip2.model.AnonymousIpResponse;
import com.maxmind.geoip2.model.AsnResponse;
import com.maxmind.geoip2.model.CityResponse;
import com.maxmind.geoip2.model.ConnectionTypeResponse;
import com.maxmind.geoip2.model.CountryResponse;
import com.maxmind.geoip2.model.DomainResponse;
import com.maxmind.geoip2.model.EnterpriseResponse;
import com.maxmind.geoip2.model.IspResponse;

import org.elasticsearch.core.Nullable;

import java.io.IOException;

/**
 * Provides a uniform interface for interacting with various ip databases.
 */
public interface IpDatabase extends AutoCloseable {

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
    CityResponse getCity(String ipAddress);

    /**
     * @param ipAddress the IP address to look up
     * @return a response containing the country data for the given address if it exists, or <code>null</code> if it could not be found
     * @throws UnsupportedOperationException may be thrown if the implementation does not support retrieving country data
     */
    @Nullable
    CountryResponse getCountry(String ipAddress);

    /**
     * @param ipAddress the IP address to look up
     * @return a response containing the Autonomous System Number for the given address if it exists, or <code>null</code> if it could not
     *         be found
     * @throws UnsupportedOperationException may be thrown if the implementation does not support retrieving ASN data
     */
    @Nullable
    AsnResponse getAsn(String ipAddress);

    @Nullable
    AnonymousIpResponse getAnonymousIp(String ipAddress);

    @Nullable
    ConnectionTypeResponse getConnectionType(String ipAddress);

    @Nullable
    DomainResponse getDomain(String ipAddress);

    @Nullable
    EnterpriseResponse getEnterprise(String ipAddress);

    @Nullable
    IspResponse getIsp(String ipAddress);

    /**
     * Releases the current database object. Called after processing a single document. Databases should be closed or returned to a
     * resource pool. No further interactions should be expected.
     *
     * @throws IOException if the implementation encounters any problem while cleaning up
     */
    @Override
    void close() throws IOException;
}

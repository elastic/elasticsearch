/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest.geoip;

import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.model.AbstractResponse;

import org.elasticsearch.common.CheckedBiFunction;
import org.elasticsearch.core.Nullable;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Optional;

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
     * Returns a response from this database's reader for the given IP address
     * @param ipAddress the address to lookup
     * @param responseProvider typically a method-reference like {@code DatabaseReader::tryCity}
     * @return a possibly-null response
     * @param <RESPONSE> the subtype of {@link AbstractResponse} that will be returned
     */
    @Nullable
    <RESPONSE extends AbstractResponse> RESPONSE getResponse(
        InetAddress ipAddress,
        CheckedBiFunction<DatabaseReader, InetAddress, Optional<RESPONSE>, Exception> responseProvider
    );

    /**
     * Releases the current database object. Called after processing a single document. Databases should be closed or returned to a
     * resource pool. No further interactions should be expected.
     * @throws IOException if the implementation encounters any problem while cleaning up
     */
    void release() throws IOException;
}

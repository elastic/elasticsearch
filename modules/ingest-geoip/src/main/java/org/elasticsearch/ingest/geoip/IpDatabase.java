/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.geoip;

import com.maxmind.db.Reader;

import org.elasticsearch.common.CheckedBiFunction;
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
     * Returns a response from this database's reader for the given IP address.
     *
     * @param ipAddress the address to lookup
     * @param responseProvider a method for extracting a response from a {@link Reader}, usually this will be a method reference
     * @return a possibly-null response
     * @param <RESPONSE> the type of response that will be returned
     */
    @Nullable
    <RESPONSE> RESPONSE getResponse(String ipAddress, CheckedBiFunction<Reader, String, RESPONSE, Exception> responseProvider);

    /**
     * Releases the current database object. Called after processing a single document. Databases should be closed or returned to a
     * resource pool. No further interactions should be expected.
     *
     * @throws IOException if the implementation encounters any problem while cleaning up
     */
    @Override
    void close() throws IOException;
}

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
     */
    @Nullable
    @Deprecated // use getTypedResponse instead
    default <RESPONSE> RESPONSE getResponse(String ipAddress, CheckedBiFunction<Reader, String, RESPONSE, Exception> responseProvider) {
        // TODO(pete): Decide what to do here
        // TODO(pete): This is one option, which makes this method work so long as it is invoked with a type which satisfies the constraint,
        // but fails nastily if it doesn't.
        // @SuppressWarnings("unchecked") // TODO(pete): Put some words of wisdom here
        // RESPONSE typedResponse = (RESPONSE) getTypedResponse(
        // ipAddress,
        // (CheckedBiFunction<Reader, String, ? extends Response, Exception>) responseProvider
        // );
        // return typedResponse;
        // TODO(pete): This is the other option, which fails fast. Which is okay as nothing calls this method.
        // TODO(pete): If we do this, add a message to the exception.
        throw new UnsupportedOperationException();
    }

    // TODO(pete): If we end up doing this, pick a better name for getTypedResponse() and document why we do this weird thing.

    /**
     * Returns a response from this database's reader for the given IP address.
     *
     * @param ipAddress the address to lookup
     * @param responseProvider a method for extracting a response from a {@link Reader}, usually this will be a method reference
     * @return a possibly-null response
     */
    @Nullable
    default <RESPONSE extends Response> RESPONSE getTypedResponse(
        String ipAddress,
        CheckedBiFunction<Reader, String, RESPONSE, Exception> responseProvider
    ) {
        return getResponse(ipAddress, responseProvider);
    }

    /**
     * Releases the current database object. Called after processing a single document. Databases should be closed or returned to a
     * resource pool. No further interactions should be expected.
     *
     * @throws IOException if the implementation encounters any problem while cleaning up
     */
    @Override
    void close() throws IOException;

    interface Response {

        // TODO PETE: Remove this default implementation and implement in all implementing classes instead
        default long sizeInBytes() {
            return 0;
        }
    }
}

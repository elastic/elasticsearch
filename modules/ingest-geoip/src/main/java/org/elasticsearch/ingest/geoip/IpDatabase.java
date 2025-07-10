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

    /*
     * TODO(pete): Add an explanation of all this to the class- and method-level javadocs.
     *
     * This class has two related methods, getResponse and a method getTypedResponse. These are identical except that getTypedResponse's
     * generic type is constrained to extend the Response interface. The getResponse method is abstract. The default implementation of
     * getTypedResponse delegates to getResponse and that is the only place in the codebase that getResponse is called: everywhere else
     * calls getTypedResponse (as of ES 9.1.0). Implementing classes therefore have a choice: they can implement getResponse; or they can
     * override the implementation of getTypedResponse, and make getResponse throw since it will now never be called.
     *
     * (The getResponse method only exists because there are implementations of this interface outside of this codebase which haven't yet
     * been converted to implement getTypedResponse instead.)
     */

    /**
     * Returns a response from this database's reader for the given IP address.
     *
     * @param ipAddress the address to lookup
     * @param responseProvider a method for extracting a response from a {@link Reader}, usually this will be a method reference
     * @return a possibly-null response
     */
    @Nullable
    @Deprecated // use getTypedResponse instead
    // TODO(pete): If we're allowed to remove this in v10, annotate it to remind us to do that.
    <RESPONSE> RESPONSE getResponse(String ipAddress, CheckedBiFunction<Reader, String, RESPONSE, Exception> responseProvider);

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

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.transport;

/**
 * Requests that implement this interface will be compressed when {@link TransportSettings#TRANSPORT_COMPRESS}
 * is configured to {@link Compression.Enabled#INDEXING_DATA} and isRawIndexingData() returns true. This is
 * intended to be requests/responses primarily composed of raw source data.
 */
public interface RawIndexingDataTransportRequest {

    default boolean isRawIndexingData() {
        return true;
    }
}

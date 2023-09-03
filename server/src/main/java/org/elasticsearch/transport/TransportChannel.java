/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.transport;

import org.elasticsearch.TransportVersion;

import java.io.IOException;

/**
 * A transport channel allows to send a response to a request on the channel.
 */
public interface TransportChannel {

    String getProfileName();

    String getChannelType();

    void sendResponse(TransportResponse response) throws IOException;

    void sendResponse(Exception exception) throws IOException;

    /**
     * Returns the version of the data to communicate in this channel.
     */
    default TransportVersion getVersion() {
        return TransportVersion.current();
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.transport;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.Version;

import java.io.IOException;

/**
 * A transport channel allows to send a response to a request on the channel.
 */
public interface TransportChannel {

    Logger logger = LogManager.getLogger(TransportChannel.class);

    String getProfileName();

    String getChannelType();

    void sendResponse(TransportResponse response) throws IOException;

    void sendResponse(Exception exception) throws IOException;

    /**
     * Returns the version of the other party that this channel will send a response to.
     */
    default Version getVersion() {
        return Version.CURRENT;
    }

    /**
     * A helper method to send an exception and handle and log a subsequent exception
     */
    static void sendErrorResponse(TransportChannel channel, String actionName, TransportRequest request, Exception e) {
        try {
            channel.sendResponse(e);
        } catch (Exception sendException) {
            sendException.addSuppressed(e);
            logger.warn(() -> new ParameterizedMessage(
                "Failed to send error response for action [{}] and request [{}]", actionName, request), sendException);
        }
    }
}

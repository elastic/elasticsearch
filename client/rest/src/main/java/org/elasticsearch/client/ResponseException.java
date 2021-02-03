/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client;

import org.apache.http.HttpEntity;
import org.apache.http.entity.BufferedHttpEntity;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.util.Locale;

/**
 * Exception thrown when an elasticsearch node responds to a request with a status code that indicates an error.
 * Holds the response that was returned.
 */
public final class ResponseException extends IOException {

    private final Response response;

    public ResponseException(Response response) throws IOException {
        super(buildMessage(response));
        this.response = response;
    }

    static String buildMessage(Response response) throws IOException {
        String message = String.format(Locale.ROOT,
            "method [%s], host [%s], URI [%s], status line [%s]",
            response.getRequestLine().getMethod(),
            response.getHost(),
            response.getRequestLine().getUri(),
            response.getStatusLine().toString()
        );

        if (response.hasWarnings()) {
            message += "\n" + "Warnings: " + response.getWarnings();
        }

        HttpEntity entity = response.getEntity();
        if (entity != null) {
            if (entity.isRepeatable() == false) {
                entity = new BufferedHttpEntity(entity);
                response.getHttpResponse().setEntity(entity);
            }
            message += "\n" + EntityUtils.toString(entity);
        }
        return message;
    }

    /**
     * Returns the {@link Response} that caused this exception to be thrown.
     */
    public Response getResponse() {
        return response;
    }
}

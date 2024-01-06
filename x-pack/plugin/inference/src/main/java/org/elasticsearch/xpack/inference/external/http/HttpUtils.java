/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http;

import org.apache.http.client.methods.HttpRequestBase;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.xpack.inference.logging.ThrottlerManager;

import static org.elasticsearch.core.Strings.format;

public class HttpUtils {

    public static void checkForFailureStatusCode(
        ThrottlerManager throttlerManager,
        Logger logger,
        HttpRequestBase request,
        HttpResult result
    ) {
        if (result.response().getStatusLine().getStatusCode() >= 300) {
            String message = getStatusCodeErrorMessage(request, result);

            throttlerManager.warn(logger, message);

            throw new IllegalStateException(message);
        }
    }

    private static String getStatusCodeErrorMessage(HttpRequestBase request, HttpResult result) {
        int statusCode = result.response().getStatusLine().getStatusCode();

        if (statusCode >= 400) {
            return format(
                "Received a failure status code for request [%s] status [%s]",
                request.getRequestLine(),
                result.response().getStatusLine().getStatusCode()
            );
        } else if (statusCode >= 300) {
            return format(
                "Unhandled redirection for request [%s] status [%s]",
                request.getRequestLine(),
                result.response().getStatusLine().getStatusCode()
            );
        } else {
            return "";
        }
    }

    public static void checkForEmptyBody(ThrottlerManager throttlerManager, Logger logger, HttpRequestBase request, HttpResult result) {
        if (result.isBodyEmpty()) {
            String message = format("Response body was empty for request [%s]", request.getRequestLine());
            throttlerManager.warn(logger, message);
            throw new IllegalStateException(message);
        }
    }

    private HttpUtils() {}
}

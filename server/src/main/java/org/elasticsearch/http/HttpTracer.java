/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.http;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;

import java.util.List;

import static org.elasticsearch.core.Strings.format;

/**
 * Http request trace logger. See {@link #maybeLogRequest(RestRequest, Exception)} for details.
 */
class HttpTracer {

    private final Logger logger = LogManager.getLogger(HttpTracer.class);

    private volatile String[] tracerLogInclude;
    private volatile String[] tracerLogExclude;

    HttpTracer(Settings settings, ClusterSettings clusterSettings) {

        setTracerLogInclude(HttpTransportSettings.SETTING_HTTP_TRACE_LOG_INCLUDE.get(settings));
        setTracerLogExclude(HttpTransportSettings.SETTING_HTTP_TRACE_LOG_EXCLUDE.get(settings));

        clusterSettings.addSettingsUpdateConsumer(HttpTransportSettings.SETTING_HTTP_TRACE_LOG_INCLUDE, this::setTracerLogInclude);
        clusterSettings.addSettingsUpdateConsumer(HttpTransportSettings.SETTING_HTTP_TRACE_LOG_EXCLUDE, this::setTracerLogExclude);
    }

    /**
     * Logs the given request if request tracing is enabled and the request uri matches the current include and exclude patterns defined
     * in {@link HttpTransportSettings#SETTING_HTTP_TRACE_LOG_INCLUDE} and {@link HttpTransportSettings#SETTING_HTTP_TRACE_LOG_EXCLUDE}.
     * If the request was logged returns a logger to log sending the response with or {@code null} otherwise.
     *
     * @param restRequest Rest request to trace
     * @param e           Exception when handling the request or {@code null} if none
     * @return            This instance to use for logging the response via {@link #logResponse} to this request if it was logged or
     *                    {@code null} if the request wasn't logged
     */
    @Nullable
    HttpTracer maybeLogRequest(RestRequest restRequest, @Nullable Exception e) {
        if (logger.isTraceEnabled() && TransportService.shouldTraceAction(restRequest.uri(), tracerLogInclude, tracerLogExclude)) {
            logger.trace(
                () -> format(
                    "[%s][%s][%s][%s] received request from [%s]",
                    restRequest.getRequestId(),
                    restRequest.header(Task.X_OPAQUE_ID_HTTP_HEADER),
                    restRequest.method(),
                    restRequest.uri(),
                    restRequest.getHttpChannel()
                ),
                e
            );
            return this;
        }
        return null;
    }

    /**
     * Logs the response to a request that was logged by {@link #maybeLogRequest(RestRequest, Exception)}.
     *
     * @param restResponse  RestResponse
     * @param httpChannel   HttpChannel the response was sent on
     * @param contentLength Value of the response content length header
     * @param opaqueHeader  Value of HTTP header {@link Task#X_OPAQUE_ID_HTTP_HEADER}
     * @param requestId     Request id as returned by {@link RestRequest#getRequestId()}
     * @param success       Whether the response was successfully sent
     */
    void logResponse(
        RestResponse restResponse,
        HttpChannel httpChannel,
        String contentLength,
        String opaqueHeader,
        long requestId,
        boolean success
    ) {
        logger.trace(
            () -> format(
                "[%s][%s][%s][%s][%s] sent response to [%s] success [%s]",
                requestId,
                opaqueHeader,
                restResponse.status(),
                restResponse.contentType(),
                contentLength,
                httpChannel,
                success
            )
        );
    }

    private void setTracerLogInclude(List<String> tracerLogInclude) {
        this.tracerLogInclude = tracerLogInclude.toArray(Strings.EMPTY_ARRAY);
    }

    private void setTracerLogExclude(List<String> tracerLogExclude) {
        this.tracerLogExclude = tracerLogExclude.toArray(Strings.EMPTY_ARRAY);
    }
}

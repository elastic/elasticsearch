/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.http;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;

import java.util.List;

/**
 * Http request trace logger. See {@link #maybeTraceRequest(RestRequest, Exception)} for details.
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
     * @return            This instance to use for logging the response via {@link #traceResponse} to this request if it was logged or
     *                    {@code null} if the request wasn't logged
     */
    @Nullable
    HttpTracer maybeTraceRequest(RestRequest restRequest, @Nullable Exception e) {
        if (logger.isTraceEnabled() && TransportService.shouldTraceAction(restRequest.uri(), tracerLogInclude, tracerLogExclude)) {
            logger.trace(new ParameterizedMessage("[{}][{}][{}][{}] received request from [{}]", restRequest.getRequestId(),
                restRequest.header(Task.X_OPAQUE_ID), restRequest.method(), restRequest.uri(), restRequest.getHttpChannel()), e);
            return this;
        }
        return null;
    }

    /**
     * Logs the response to a request that was logged by {@link #maybeTraceRequest(RestRequest, Exception)}.
     *
     * @param restResponse  RestResponse
     * @param httpChannel   HttpChannel the response was sent on
     * @param contentLength Value of the response content length header
     * @param opaqueHeader  Value of HTTP header {@link Task#X_OPAQUE_ID}
     * @param requestId     Request id as returned by {@link RestRequest#getRequestId()}
     * @param success       Whether the response was successfully sent
     */
    void traceResponse(RestResponse restResponse, HttpChannel httpChannel, String contentLength, String opaqueHeader, long requestId,
                       boolean success) {
        logger.trace(new ParameterizedMessage("[{}][{}][{}][{}][{}] sent response to [{}] success [{}]", requestId,
            opaqueHeader, restResponse.status(), restResponse.contentType(), contentLength, httpChannel, success));
    }

    private void setTracerLogInclude(List<String> tracerLogInclude) {
        this.tracerLogInclude = tracerLogInclude.toArray(Strings.EMPTY_ARRAY);
    }

    private void setTracerLogExclude(List<String> tracerLogExclude) {
        this.tracerLogExclude = tracerLogExclude.toArray(Strings.EMPTY_ARRAY);
    }
}

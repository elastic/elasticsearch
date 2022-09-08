/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.http;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.rest.ChunkedRestResponseBody;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;

import java.util.List;
import java.util.Map;

/**
 * A basic http request abstraction. Http modules needs to implement this interface to integrate with the
 * server package's rest handling.
 */
public interface HttpRequest {

    enum HttpVersion {
        HTTP_1_0,
        HTTP_1_1
    }

    /**
     * Returns the HTTP method used in the HTTP request.
     *
     * @return the {@link RestRequest.Method} used in the REST request
     * @throws IllegalArgumentException if the HTTP method is invalid
     */
    RestRequest.Method method();

    /**
     * The uri of the rest request, with the query string.
     */
    String uri();

    BytesReference content();

    /**
     * Get all of the headers and values associated with the headers. Modifications of this map are not supported.
     */
    Map<String, List<String>> getHeaders();

    default String header(String name) {
        List<String> values = getHeaders().get(name);
        if (values != null && values.isEmpty() == false) {
            return values.get(0);
        }
        return null;
    }

    List<String> strictCookies();

    HttpVersion protocolVersion();

    HttpRequest removeHeader(String header);

    /**
     * Create an http response from this request and the supplied status and content.
     */
    HttpResponse createResponse(RestStatus status, BytesReference content);

    HttpResponse createResponse(RestStatus status, ChunkedRestResponseBody content);

    @Nullable
    Exception getInboundException();

    /**
     * Release any resources associated with this request. Implementations should be idempotent. The behavior of {@link #content()}
     * after this method has been invoked is undefined and implementation specific.
     */
    void release();

    /**
     * If this instances uses any pooled resources, creates a copy of this instance that does not use any pooled resources and releases
     * any resources associated with this instance. If the instance does not use any shared resources, returns itself.
     * @return a safe unpooled http request
     */
    HttpRequest releaseAndCopy();
}

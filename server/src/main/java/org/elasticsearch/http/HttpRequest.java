/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.http;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.rest.ChunkedRestResponseBodyPart;
import org.elasticsearch.rest.RestStatus;

import java.util.List;

/**
 * A basic http request abstraction. Http modules needs to implement this interface to integrate with the
 * server package's rest handling. This interface exposes the request's content as well as methods to be used
 * to generate a response.
 */
public interface HttpRequest extends HttpPreRequest {

    enum HttpVersion {
        HTTP_1_0,
        HTTP_1_1
    }

    HttpBody body();

    List<String> strictCookies();

    HttpVersion protocolVersion();

    HttpRequest removeHeader(String header);

    /**
     * Create an http response from this request and the supplied status and content.
     */
    HttpResponse createResponse(RestStatus status, BytesReference content);

    HttpResponse createResponse(RestStatus status, ChunkedRestResponseBodyPart firstBodyPart);

    @Nullable
    Exception getInboundException();

    /**
     * Release any resources associated with this request. Implementations should be idempotent. The behavior of {@link #body()}
     * after this method has been invoked is undefined and implementation specific.
     */
    void release();

}

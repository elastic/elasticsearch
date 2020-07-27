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

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.bytes.BytesReference;
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

    List<String> strictCookies();

    HttpVersion protocolVersion();

    HttpRequest removeHeader(String header);

    /**
     * Create an http response from this request and the supplied status and content.
     */
    HttpResponse createResponse(RestStatus status, BytesReference content);

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

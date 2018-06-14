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

package org.elasticsearch.http.netty4;

import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.http.HttpPipelinedMessage;
import org.elasticsearch.http.HttpResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.transport.netty4.Netty4Utils;

import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;

public class Netty4HttpResponse extends DefaultFullHttpResponse implements HttpResponse, HttpPipelinedMessage {

    private final int sequence;
    private final Netty4HttpRequest request;

    Netty4HttpResponse(Netty4HttpRequest request, RestStatus status, BytesReference content) {
        super(request.nettyRequest().protocolVersion(), getStatus(status), Netty4Utils.toByteBuf(content));
        this.sequence = request.sequence();
        this.request = request;
    }

    @Override
    public void addHeader(String name, String value) {
        headers().add(name, value);
    }

    @Override
    public boolean containsHeader(String name) {
        return headers().contains(name);
    }

    @Override
    public int getSequence() {
        return sequence;
    }

    public Netty4HttpRequest getRequest() {
        return request;
    }

    private static Map<RestStatus, HttpResponseStatus> MAP;

    static {
        EnumMap<RestStatus, HttpResponseStatus> map = new EnumMap<>(RestStatus.class);
        map.put(RestStatus.CONTINUE, HttpResponseStatus.CONTINUE);
        map.put(RestStatus.SWITCHING_PROTOCOLS, HttpResponseStatus.SWITCHING_PROTOCOLS);
        map.put(RestStatus.OK, HttpResponseStatus.OK);
        map.put(RestStatus.CREATED, HttpResponseStatus.CREATED);
        map.put(RestStatus.ACCEPTED, HttpResponseStatus.ACCEPTED);
        map.put(RestStatus.NON_AUTHORITATIVE_INFORMATION, HttpResponseStatus.NON_AUTHORITATIVE_INFORMATION);
        map.put(RestStatus.NO_CONTENT, HttpResponseStatus.NO_CONTENT);
        map.put(RestStatus.RESET_CONTENT, HttpResponseStatus.RESET_CONTENT);
        map.put(RestStatus.PARTIAL_CONTENT, HttpResponseStatus.PARTIAL_CONTENT);
        map.put(RestStatus.MULTI_STATUS, HttpResponseStatus.INTERNAL_SERVER_ERROR); // no status for this??
        map.put(RestStatus.MULTIPLE_CHOICES, HttpResponseStatus.MULTIPLE_CHOICES);
        map.put(RestStatus.MOVED_PERMANENTLY, HttpResponseStatus.MOVED_PERMANENTLY);
        map.put(RestStatus.FOUND, HttpResponseStatus.FOUND);
        map.put(RestStatus.SEE_OTHER, HttpResponseStatus.SEE_OTHER);
        map.put(RestStatus.NOT_MODIFIED, HttpResponseStatus.NOT_MODIFIED);
        map.put(RestStatus.USE_PROXY, HttpResponseStatus.USE_PROXY);
        map.put(RestStatus.TEMPORARY_REDIRECT, HttpResponseStatus.TEMPORARY_REDIRECT);
        map.put(RestStatus.BAD_REQUEST, HttpResponseStatus.BAD_REQUEST);
        map.put(RestStatus.UNAUTHORIZED, HttpResponseStatus.UNAUTHORIZED);
        map.put(RestStatus.PAYMENT_REQUIRED, HttpResponseStatus.PAYMENT_REQUIRED);
        map.put(RestStatus.FORBIDDEN, HttpResponseStatus.FORBIDDEN);
        map.put(RestStatus.NOT_FOUND, HttpResponseStatus.NOT_FOUND);
        map.put(RestStatus.METHOD_NOT_ALLOWED, HttpResponseStatus.METHOD_NOT_ALLOWED);
        map.put(RestStatus.NOT_ACCEPTABLE, HttpResponseStatus.NOT_ACCEPTABLE);
        map.put(RestStatus.PROXY_AUTHENTICATION, HttpResponseStatus.PROXY_AUTHENTICATION_REQUIRED);
        map.put(RestStatus.REQUEST_TIMEOUT, HttpResponseStatus.REQUEST_TIMEOUT);
        map.put(RestStatus.CONFLICT, HttpResponseStatus.CONFLICT);
        map.put(RestStatus.GONE, HttpResponseStatus.GONE);
        map.put(RestStatus.LENGTH_REQUIRED, HttpResponseStatus.LENGTH_REQUIRED);
        map.put(RestStatus.PRECONDITION_FAILED, HttpResponseStatus.PRECONDITION_FAILED);
        map.put(RestStatus.REQUEST_ENTITY_TOO_LARGE, HttpResponseStatus.REQUEST_ENTITY_TOO_LARGE);
        map.put(RestStatus.REQUEST_URI_TOO_LONG, HttpResponseStatus.REQUEST_URI_TOO_LONG);
        map.put(RestStatus.UNSUPPORTED_MEDIA_TYPE, HttpResponseStatus.UNSUPPORTED_MEDIA_TYPE);
        map.put(RestStatus.REQUESTED_RANGE_NOT_SATISFIED, HttpResponseStatus.REQUESTED_RANGE_NOT_SATISFIABLE);
        map.put(RestStatus.EXPECTATION_FAILED, HttpResponseStatus.EXPECTATION_FAILED);
        map.put(RestStatus.UNPROCESSABLE_ENTITY, HttpResponseStatus.BAD_REQUEST);
        map.put(RestStatus.LOCKED, HttpResponseStatus.BAD_REQUEST);
        map.put(RestStatus.FAILED_DEPENDENCY, HttpResponseStatus.BAD_REQUEST);
        map.put(RestStatus.TOO_MANY_REQUESTS, HttpResponseStatus.TOO_MANY_REQUESTS);
        map.put(RestStatus.INTERNAL_SERVER_ERROR, HttpResponseStatus.INTERNAL_SERVER_ERROR);
        map.put(RestStatus.NOT_IMPLEMENTED, HttpResponseStatus.NOT_IMPLEMENTED);
        map.put(RestStatus.BAD_GATEWAY, HttpResponseStatus.BAD_GATEWAY);
        map.put(RestStatus.SERVICE_UNAVAILABLE, HttpResponseStatus.SERVICE_UNAVAILABLE);
        map.put(RestStatus.GATEWAY_TIMEOUT, HttpResponseStatus.GATEWAY_TIMEOUT);
        map.put(RestStatus.HTTP_VERSION_NOT_SUPPORTED, HttpResponseStatus.HTTP_VERSION_NOT_SUPPORTED);
        MAP = Collections.unmodifiableMap(map);
    }

    private static HttpResponseStatus getStatus(RestStatus status) {
        return MAP.getOrDefault(status, HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }

}


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
package org.elasticsearch.rest;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.http.HttpChannel;
import org.elasticsearch.http.HttpRequest;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class NewRestRequest extends RestRequest {

    private final HttpRequest httpRequest;
    private final HttpChannel httpChannel;

    /**
     * Creates a new REST request.
     *
     * @param xContentRegistry the content registry
     * @param httpRequest      the underlying http request
     * @param httpChannel      the underlying http channel
     * @throws BadParameterException      if the parameters can not be decoded
     * @throws ContentTypeHeaderException if the Content-Type header can not be parsed
     */
    private NewRestRequest(NamedXContentRegistry xContentRegistry, HttpRequest httpRequest, HttpChannel httpChannel) {
        super(xContentRegistry, httpRequest.uri(), httpRequest.getHeaders());
        this.httpRequest = httpRequest;
        this.httpChannel = httpChannel;
    }

    private NewRestRequest(NamedXContentRegistry xContentRegistry, HttpRequest httpRequest, HttpChannel httpChannel,
                           Map<String, List<String>> headers, Map<String, String> params) {
        super(xContentRegistry, params, httpRequest.uri(), headers);
        this.httpRequest = httpRequest;
        this.httpChannel = httpChannel;
    }

    @Override
    public Method method() {
        return httpRequest.method();
    }

    @Override
    public String uri() {
        return httpRequest.uri();
    }

    @Override
    public boolean hasContent() {
        return content().length() > 0;
    }

    @Override
    public BytesReference content() {
        return httpRequest.content();
    }

    @Override
    public HttpRequest getHttpRequest() {
        return httpRequest;
    }

    public HttpRequest httpRequest() {
        return httpRequest;
    }

    public static NewRestRequest request(NamedXContentRegistry xContentRegistry, HttpRequest httpRequest, HttpChannel httpChannel) {
        return new NewRestRequest(xContentRegistry, httpRequest, httpChannel);
    }

    public static NewRestRequest requestWithoutParameters(NamedXContentRegistry xContentRegistry, HttpRequest httpRequest,
                                                          HttpChannel httpChannel) {
        return new NewRestRequest(xContentRegistry, httpRequest, httpChannel, httpRequest.getHeaders(), Collections.emptyMap());
    }
}

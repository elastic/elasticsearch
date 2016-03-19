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
package org.elasticsearch.test.rest.client.http;

import org.apache.http.Header;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Response obtained from an http request
 * Always consumes the whole response body loading it entirely into a string
 */
public class HttpResponse {

    private static final ESLogger logger = Loggers.getLogger(HttpResponse.class);

    private final HttpUriRequest httpRequest;
    private final int statusCode;
    private final String reasonPhrase;
    private final String body;
    private final Map<String, String> headers = new HashMap<>();

    HttpResponse(HttpUriRequest httpRequest, CloseableHttpResponse httpResponse) {
        this.httpRequest = httpRequest;
        this.statusCode = httpResponse.getStatusLine().getStatusCode();
        this.reasonPhrase = httpResponse.getStatusLine().getReasonPhrase();
        for (Header header : httpResponse.getAllHeaders()) {
            this.headers.put(header.getName(), header.getValue());
        }
        if (httpResponse.getEntity() != null) {
            try {
                this.body = EntityUtils.toString(httpResponse.getEntity(), HttpRequestBuilder.DEFAULT_CHARSET);
            } catch (IOException e) {
                EntityUtils.consumeQuietly(httpResponse.getEntity());
                throw new RuntimeException(e);
            } finally {
                try {
                    httpResponse.close();
                } catch (IOException e) {
                    logger.error("Failed closing response", e);
                }
            }
        } else {
            this.body = null;
        }
    }

    public boolean isError() {
        return statusCode >= 400;
    }

    public int getStatusCode() {
        return statusCode;
    }

    public String getReasonPhrase() {
        return reasonPhrase;
    }

    public String getBody() {
        return body;
    }

    public boolean hasBody() {
        return body != null;
    }

    public boolean supportsBody() {
        return !HttpHead.METHOD_NAME.equals(httpRequest.getMethod());
    }

    public Map<String, String> getHeaders() {
        return headers;
    }

    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder(statusCode).append(" ").append(reasonPhrase);
        if (hasBody()) {
            stringBuilder.append("\n").append(body);
        }
        return stringBuilder.toString();
    }
}

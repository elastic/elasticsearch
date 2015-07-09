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

import org.elasticsearch.common.Strings;

import java.util.List;
import java.util.Map;

public class HttpResponse {
    private final String verb;
    private final String body;
    private final int statusCode;
    private final String reasonPhrase;
    private Map<String, List<String>> headers;
    private final Throwable e;

    public HttpResponse(String verb, String body, int statusCode, String reasonPhrase, Map<String, List<String>> headers, Throwable e) {
        this.verb = verb;
        this.body = body;
        this.statusCode = statusCode;
        this.reasonPhrase = reasonPhrase;
        this.headers = headers;
        this.e = e;
    }

    public String getBody() {
        return body;
    }

    public int getStatusCode() {
        return statusCode;
    }

    public boolean isError() {
        return statusCode >= 300;
    }

    public boolean hasBody() {
        return Strings.hasText(body);
    }

    public boolean supportsBody() {
        return verb.equals("HEAD") == false;
    }

    public String getReasonPhrase() {
        return reasonPhrase;
    }

    public Throwable cause() {
        return e;
    }

    public Map<String, List<String>> getHeaders() {
        return headers;
    }

    public String getHeader(String name) {
        if (headers == null) {
            return null;
        }
        List<String> vals = headers.get(name);
        if (vals == null || vals.size() == 0) {
            return null;
        }
        return vals.iterator().next();
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

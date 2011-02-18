/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.wares;

import org.elasticsearch.common.Unicode;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.rest.support.AbstractRestRequest;
import org.elasticsearch.rest.support.RestUtils;

import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class ServletRestRequest extends AbstractRestRequest implements org.elasticsearch.rest.RestRequest {

    private final HttpServletRequest servletRequest;

    private final Method method;

    private final Map<String, String> params;

    private final byte[] content;

    public ServletRestRequest(HttpServletRequest servletRequest) throws IOException {
        this.servletRequest = servletRequest;
        this.method = Method.valueOf(servletRequest.getMethod());
        this.params = new HashMap<String, String>();

        if (servletRequest.getQueryString() != null) {
            RestUtils.decodeQueryString(servletRequest.getQueryString(), 0, params);
        }

        content = Streams.copyToByteArray(servletRequest.getInputStream());
    }

    @Override public Method method() {
        return this.method;
    }

    @Override public String uri() {
        return servletRequest.getRequestURI().substring(servletRequest.getContextPath().length() + servletRequest.getServletPath().length());
    }

    @Override public String rawPath() {
        return servletRequest.getRequestURI().substring(servletRequest.getContextPath().length() + servletRequest.getServletPath().length());
    }

    @Override public boolean hasContent() {
        return content.length > 0;
    }

    @Override public boolean contentUnsafe() {
        return false;
    }

    @Override public byte[] contentByteArray() {
        return content;
    }

    @Override public int contentByteArrayOffset() {
        return 0;
    }

    @Override public int contentLength() {
        return content.length;
    }

    @Override public String contentAsString() {
        return Unicode.fromBytes(contentByteArray(), contentByteArrayOffset(), contentLength());
    }

    @Override public String header(String name) {
        return servletRequest.getHeader(name);
    }

    @Override public Map<String, String> params() {
        return params;
    }

    @Override public boolean hasParam(String key) {
        return params.containsKey(key);
    }

    @Override public String param(String key) {
        return params.get(key);
    }

    @Override public String param(String key, String defaultValue) {
        String value = params.get(key);
        if (value == null) {
            return defaultValue;
        }
        return value;
    }
}
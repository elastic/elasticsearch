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

package org.elasticsearch.http.netty;

import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.http.HttpRequest;
import org.elasticsearch.util.Booleans;
import org.elasticsearch.util.SizeValue;
import org.elasticsearch.util.TimeValue;
import org.jboss.netty.buffer.ChannelBufferInputStream;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.QueryStringDecoder;

import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import static org.elasticsearch.util.SizeValue.*;
import static org.elasticsearch.util.TimeValue.*;

/**
 * @author kimchy (Shay Banon)
 */
public class NettyHttpRequest implements HttpRequest {

    private final Pattern commaPattern = Pattern.compile(",");

    private final org.jboss.netty.handler.codec.http.HttpRequest request;

    private QueryStringDecoder queryStringDecoder;

    public NettyHttpRequest(org.jboss.netty.handler.codec.http.HttpRequest request) {
        this.request = request;
        this.queryStringDecoder = new QueryStringDecoder(request.getUri());
    }

    @Override public Method method() {
        HttpMethod httpMethod = request.getMethod();
        if (httpMethod == HttpMethod.GET)
            return Method.GET;

        if (httpMethod == HttpMethod.POST)
            return Method.POST;

        if (httpMethod == HttpMethod.PUT)
            return Method.PUT;

        if (httpMethod == HttpMethod.DELETE)
            return Method.DELETE;

        return Method.GET;
    }

    @Override public String uri() {
        return request.getUri();
    }

    @Override public boolean hasContent() {
        return request.getContent().readableBytes() > 0;
    }

    @Override public InputStream contentAsStream() {
        return new ChannelBufferInputStream(request.getContent());
    }

    @Override public byte[] contentAsBytes() {
        byte[] data = new byte[request.getContent().readableBytes()];
        request.getContent().getBytes(request.getContent().readerIndex(), data);
        return data;
    }

    @Override public String contentAsString() {
        return request.getContent().toString("UTF-8");
    }

    @Override public Set<String> headerNames() {
        return request.getHeaderNames();
    }

    @Override public String header(String name) {
        return request.getHeader(name);
    }

    @Override public List<String> headers(String name) {
        return request.getHeaders(name);
    }

    @Override public String cookie() {
        return request.getHeader(HttpHeaders.Names.COOKIE);
    }

    @Override public float paramAsFloat(String key, float defaultValue) {
        String sValue = param(key);
        if (sValue == null) {
            return defaultValue;
        }
        try {
            return Float.parseFloat(sValue);
        } catch (NumberFormatException e) {
            throw new ElasticSearchIllegalArgumentException("Failed to parse float parameter [" + key + "] with value [" + sValue + "]", e);
        }
    }

    @Override public int paramAsInt(String key, int defaultValue) {
        String sValue = param(key);
        if (sValue == null) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(sValue);
        } catch (NumberFormatException e) {
            throw new ElasticSearchIllegalArgumentException("Failed to parse int parameter [" + key + "] with value [" + sValue + "]", e);
        }
    }

    @Override public boolean paramAsBoolean(String key, boolean defaultValue) {
        return Booleans.parseBoolean(param(key), defaultValue);
    }

    @Override public Boolean paramAsBoolean(String key, Boolean defaultValue) {
        String sValue = param(key);
        if (sValue == null) {
            return defaultValue;
        }
        return !(sValue.equals("false") || sValue.equals("0") || sValue.equals("off"));
    }

    @Override public TimeValue paramAsTime(String key, TimeValue defaultValue) {
        return parseTimeValue(param(key), defaultValue);
    }

    @Override public SizeValue paramAsSize(String key, SizeValue defaultValue) {
        return parseSizeValue(param(key), defaultValue);
    }

    @Override public String[] paramAsStringArray(String key, String[] defaultValue) {
        String value = param(key);
        if (value == null) {
            return defaultValue;
        }
        return commaPattern.split(value);
    }

    @Override public boolean hasParam(String key) {
        return queryStringDecoder.getParameters().containsKey(key);
    }

    @Override public String param(String key) {
        List<String> keyParams = params(key);
        if (keyParams == null || keyParams.isEmpty()) {
            return null;
        }
        return keyParams.get(0);
    }

    @Override public List<String> params(String key) {
        return queryStringDecoder.getParameters().get(key);
    }

    @Override public Map<String, List<String>> params() {
        return queryStringDecoder.getParameters();
    }
}

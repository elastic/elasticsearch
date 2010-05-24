/*
 * Copyright 2010 Ning, Inc.
 *
 * Ning licenses this file to you under the Apache License, version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at:
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.util.http.client;

import org.elasticsearch.util.collect.Multimap;
import org.elasticsearch.util.http.client.Request.EntityWriter;

import java.io.InputStream;
import java.util.Map;

/**
 * Builder for a {@link Request}.
 */
public class RequestBuilder extends RequestBuilderBase<RequestBuilder> {
    public RequestBuilder(RequestType type) {
        super(RequestBuilder.class, type);
    }

    public RequestBuilder(Request prototype) {
        super(RequestBuilder.class, prototype);
    }

    // Note: For now we keep the delegates in place even though they are not needed
    //       since otherwise Clojure (and maybe other languages) won't be able to
    //       access these methods - see Clojure tickets 126 and 259

    @Override
    public RequestBuilder addBodyPart(Part part) throws IllegalArgumentException {
        return super.addBodyPart(part);
    }

    @Override
    public RequestBuilder addCookie(Cookie cookie) {
        return super.addCookie(cookie);
    }

    @Override
    public RequestBuilder addHeader(String name, String value) {
        return super.addHeader(name, value);
    }

    @Override
    public RequestBuilder addParameter(String key, String value) throws IllegalArgumentException {
        return super.addParameter(key, value);
    }

    @Override
    public RequestBuilder addQueryParameter(String name, String value) {
        return super.addQueryParameter(name, value);
    }

    @Override
    public Request build() {
        return super.build();
    }

    @Override
    public RequestBuilder setBody(byte[] data) throws IllegalArgumentException {
        return super.setBody(data);
    }

    @Override
    public RequestBuilder setBody(EntityWriter dataWriter, long length) throws IllegalArgumentException {
        return super.setBody(dataWriter, length);
    }

    @Override
    public RequestBuilder setBody(EntityWriter dataWriter) {
        return super.setBody(dataWriter);
    }

    @Override
    public RequestBuilder setBody(InputStream stream) throws IllegalArgumentException {
        return super.setBody(stream);
    }

    @Override
    public RequestBuilder setBody(String data) throws IllegalArgumentException {
        return super.setBody(data);
    }

    @Override
    public RequestBuilder setHeader(String name, String value) {
        return super.setHeader(name, value);
    }

    @Override
    public RequestBuilder setHeaders(Headers headers) {
        return super.setHeaders(headers);
    }

    @Override
    public RequestBuilder setParameters(Map<String, String> parameters) throws IllegalArgumentException {
        return super.setParameters(parameters);
    }

    @Override
    public RequestBuilder setParameters(Multimap<String, String> parameters) throws IllegalArgumentException {
        return super.setParameters(parameters);
    }

    @Override
    public RequestBuilder setUrl(String url) {
        return super.setUrl(url);
    }

    @Override
    public RequestBuilder setVirtualHost(String virtualHost) {
        return super.setVirtualHost(virtualHost);
    }
}

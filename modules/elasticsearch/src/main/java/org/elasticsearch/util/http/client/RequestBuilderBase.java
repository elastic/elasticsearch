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

import org.elasticsearch.util.collect.LinkedListMultimap;
import org.elasticsearch.util.collect.Multimap;
import org.elasticsearch.util.collect.Multimaps;
import org.elasticsearch.util.http.client.Request.EntityWriter;
import org.elasticsearch.util.http.collection.Pair;
import org.elasticsearch.util.http.url.Url;

import java.io.InputStream;
import java.net.MalformedURLException;
import java.util.*;

/**
 * Builder for {@link Request}
 *
 * @param <T>
 */
abstract class RequestBuilderBase<T extends RequestBuilderBase<T>> {

    private static final class RequestImpl implements Request {
        private RequestType type;
        private String url;
        private Headers headers = new Headers();
        private Collection<Cookie> cookies = new ArrayList<Cookie>();
        private byte[] byteData;
        private String stringData;
        private InputStream streamData;
        private EntityWriter entityWriter;
        private Multimap<String, String> params;
        private List<Part> parts;
        private String virtualHost;
        private long length = -1;
        public Multimap<String, String> queryParams;

        public RequestImpl() {
        }

        public RequestImpl(Request prototype) {
            if (prototype != null) {
                this.type = prototype.getType();
                this.url = prototype.getUrl();
                this.headers = new Headers(prototype.getHeaders());
                this.cookies = new ArrayList<Cookie>(prototype.getCookies());
                this.byteData = prototype.getByteData();
                this.stringData = prototype.getStringData();
                this.streamData = prototype.getStreamData();
                this.entityWriter = prototype.getEntityWriter();
                this.params = (prototype.getParams() == null ? null : LinkedListMultimap.create(prototype.getParams()));
                this.queryParams = (prototype.getQueryParams() == null ? null : LinkedListMultimap.create(prototype.getQueryParams()));
                this.parts = (prototype.getParts() == null ? null : new ArrayList<Part>(prototype.getParts()));
                this.virtualHost = prototype.getVirtualHost();
                this.length = prototype.getLength();
            }
        }

        /* @Override */

        public RequestType getType() {
            return type;
        }

        /* @Override */

        public String getUrl() {
            try {
                Url url = Url.valueOf(this.url);

                if (queryParams != null) {

                    for (Map.Entry<String, String> entry : queryParams.entries()) {
                        url.addParameter(entry.getKey(), entry.getValue());
                    }
                }

                return url.toString();
            }
            catch (MalformedURLException e) {
                throw new IllegalArgumentException("Illegal URL", e);
            }
        }

        /* @Override */

        public Headers getHeaders() {
            return Headers.unmodifiableHeaders(headers);
        }

        /* @Override */

        public Collection<Cookie> getCookies() {
            return Collections.unmodifiableCollection(cookies);
        }

        /* @Override */

        public byte[] getByteData() {
            return byteData;
        }

        /* @Override */

        public String getStringData() {
            return stringData;
        }

        /* @Override */

        public InputStream getStreamData() {
            return streamData;
        }

        /* @Override */

        public EntityWriter getEntityWriter() {
            return entityWriter;
        }

        /* @Override */

        public long getLength() {
            return length;
        }

        /* @Override */

        public Multimap<String, String> getParams() {
            return params == null ? null : Multimaps.unmodifiableMultimap(params);
        }

        /* @Override */

        public List<Part> getParts() {
            return parts == null ? null : Collections.unmodifiableList(parts);
        }

        /* @Override */

        public String getVirtualHost() {
            return virtualHost;
        }

        public Multimap<String, String> getQueryParams() {
            return queryParams == null ? null : Multimaps.unmodifiableMultimap(queryParams);
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder(url);

            sb.append("\t");
            sb.append(type);
            for (Pair<String, String> header : headers) {
                sb.append("\t");
                sb.append(header.getFirst());
                sb.append(":");
                sb.append(header.getSecond());
            }

            return sb.toString();
        }
    }

    private final Class<T> derived;
    private final RequestImpl request;

    RequestBuilderBase(Class<T> derived, RequestType type) {
        this.derived = derived;
        request = new RequestImpl();
        request.type = type;
    }

    RequestBuilderBase(Class<T> derived, Request prototype) {
        this.derived = derived;
        request = new RequestImpl(prototype);
    }

    public T setUrl(String url) {
        request.url = url;
        return derived.cast(this);
    }

    public T setVirtualHost(String virtualHost) {
        request.virtualHost = virtualHost;
        return derived.cast(this);
    }

    public T setHeader(String name, String value) {
        request.headers.replace(name, value);
        return derived.cast(this);
    }

    public T addHeader(String name, String value) {
        request.headers.add(name, value);
        return derived.cast(this);
    }

    public T setHeaders(Headers headers) {
        request.headers = (headers == null ? new Headers() : new Headers(headers));
        return derived.cast(this);
    }

    public T addCookie(Cookie cookie) {
        request.cookies.add(cookie);
        return derived.cast(this);
    }

    private void resetParameters() {
        request.params = null;
    }

    private void resetNonMultipartData() {
        request.byteData = null;
        request.stringData = null;
        request.streamData = null;
        request.entityWriter = null;
        request.length = -1;
    }

    private void resetMultipartData() {
        request.parts = null;
    }

    public T setBody(byte[] data) throws IllegalArgumentException {
        if ((request.type != RequestType.POST) && (request.type != RequestType.PUT)) {
            throw new IllegalArgumentException("Request type has to POST or PUT for content");
        }
        resetParameters();
        resetNonMultipartData();
        resetMultipartData();
        request.byteData = data;
        return derived.cast(this);
    }

    public T setBody(String data) throws IllegalArgumentException {
        if ((request.type != RequestType.POST) && (request.type != RequestType.PUT)) {
            throw new IllegalArgumentException("Request type has to POST or PUT for content");
        }
        resetParameters();
        resetNonMultipartData();
        resetMultipartData();
        request.stringData = data;
        return derived.cast(this);
    }

    public T setBody(InputStream stream) throws IllegalArgumentException {
        if ((request.type != RequestType.POST) && (request.type != RequestType.PUT)) {
            throw new IllegalArgumentException("Request type has to POST or PUT for content");
        }
        resetParameters();
        resetNonMultipartData();
        resetMultipartData();
        request.streamData = stream;
        return derived.cast(this);
    }

    public T setBody(EntityWriter dataWriter) {
        return setBody(dataWriter, -1);
    }

    public T setBody(EntityWriter dataWriter, long length) throws IllegalArgumentException {
        if ((request.type != RequestType.POST) && (request.type != RequestType.PUT)) {
            throw new IllegalArgumentException("Request type has to POST or PUT for content");
        }
        resetParameters();
        resetNonMultipartData();
        resetMultipartData();
        request.entityWriter = dataWriter;
        request.length = length;
        return derived.cast(this);
    }

    public T addQueryParameter(String name, String value) {
        if (request.queryParams == null) {
            request.queryParams = LinkedListMultimap.create();
        }
        request.queryParams.put(name, value);
        return derived.cast(this);
    }

    public T addParameter(String key, String value) throws IllegalArgumentException {
        if ((request.type != RequestType.POST) && (request.type != RequestType.PUT)) {
            throw new IllegalArgumentException("Request type has to POST or PUT for form parameters");
        }
        resetNonMultipartData();
        resetMultipartData();
        if (request.params == null) {
            request.params = LinkedListMultimap.create();
        }
        request.params.put(key, value);
        return derived.cast(this);
    }

    public T setParameters(Multimap<String, String> parameters) throws IllegalArgumentException {
        if ((request.type != RequestType.POST) && (request.type != RequestType.PUT)) {
            throw new IllegalArgumentException("Request type has to POST or PUT for form parameters");
        }
        resetNonMultipartData();
        resetMultipartData();
        request.params = LinkedListMultimap.create(parameters);
        return derived.cast(this);
    }

    public T setParameters(Map<String, String> parameters) throws IllegalArgumentException {
        if ((request.type != RequestType.POST) && (request.type != RequestType.PUT)) {
            throw new IllegalArgumentException("Request type has to POST or PUT for form parameters");
        }
        resetNonMultipartData();
        resetMultipartData();
        request.params = LinkedListMultimap.create(Multimaps.forMap(parameters));
        return derived.cast(this);
    }

    public T addBodyPart(Part part) throws IllegalArgumentException {
        if ((request.type != RequestType.POST) && (request.type != RequestType.PUT)) {
            throw new IllegalArgumentException("Request type has to POST or PUT for parts");
        }
        resetParameters();
        resetNonMultipartData();
        if (request.parts == null) {
            request.parts = new ArrayList<Part>();
        }
        request.parts.add(part);
        return derived.cast(this);
    }

    public Request build() {
        if ((request.length < 0) && (request.streamData == null) &&
                ((request.type == RequestType.POST) || (request.type == RequestType.PUT))) {
            String contentLength = request.headers.getHeaderValue("Content-Length");

            if (contentLength != null) {
                try {
                    request.length = Long.parseLong(contentLength);
                }
                catch (NumberFormatException e) {
                    // NoOp -- we wdn't specify length so it will be chunked?
                }
            }
        }
        return request;
    }
}

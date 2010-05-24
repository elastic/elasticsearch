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
package org.elasticsearch.util.http.client.providers;

import org.elasticsearch.util.http.client.AsyncHttpProvider;
import org.elasticsearch.util.http.client.Headers;
import org.elasticsearch.util.http.client.HttpResponseHeaders;
import org.elasticsearch.util.http.url.Url;
import org.jboss.netty.handler.codec.http.HttpChunkTrailer;
import org.jboss.netty.handler.codec.http.HttpResponse;

/**
 * A class that represent the HTTP headers.
 */
public class ResponseHeaders extends HttpResponseHeaders<HttpResponse> {

    private final HttpChunkTrailer trailingHeaders;

    public ResponseHeaders(Url url, HttpResponse response, AsyncHttpProvider<HttpResponse> provider) {
        super(url, response, provider, false);
        this.trailingHeaders = null;

    }

    public ResponseHeaders(Url url, HttpResponse response, AsyncHttpProvider<HttpResponse> provider, HttpChunkTrailer traillingHeaders) {
        super(url, response, provider, true);
        this.trailingHeaders = traillingHeaders;
    }

    /**
     * Return the HTTP header
     *
     * @return an {@link org.elasticsearch.util.http.client.Headers}
     */
    public Headers getHeaders() {
        Headers h = new Headers();
        for (String s : response.getHeaderNames()) {
            for (String header : response.getHeaders(s)) {
                h.add(s, header);
            }
        }

        if (trailingHeaders != null && trailingHeaders.getHeaderNames().size() > 0) {
            for (final String s : trailingHeaders.getHeaderNames()) {
                for (String header : response.getHeaders(s)) {
                    h.add(s, header);
                }
            }
        }

        return Headers.unmodifiableHeaders(h);
    }

}
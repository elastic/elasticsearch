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
import org.elasticsearch.util.http.client.HttpResponseBodyPart;
import org.elasticsearch.util.http.url.Url;
import org.jboss.netty.handler.codec.http.HttpChunk;
import org.jboss.netty.handler.codec.http.HttpResponse;

/**
 * A callback class used when an HTTP response body is received.
 */
public class ResponseBodyPart extends HttpResponseBodyPart<HttpResponse> {

    private final HttpChunk chunk;

    public ResponseBodyPart(Url url, HttpResponse response, AsyncHttpProvider<HttpResponse> provider) {
        super(url, response, provider);
        this.chunk = null;
    }

    public ResponseBodyPart(Url url, HttpResponse response, AsyncHttpProvider<HttpResponse> provider, HttpChunk chunk) {
        super(url, response, provider);
        this.chunk = chunk;
    }

    /**
     * Return the response body's part bytes received.
     *
     * @return the response body's part bytes received.
     */
    public byte[] getBodyPartBytes() {
        if (chunk != null) {
            return chunk.getContent().array();
        } else {
            return response.getContent().array();
        }
    }

    protected HttpChunk chunk() {
        return chunk;
    }
}
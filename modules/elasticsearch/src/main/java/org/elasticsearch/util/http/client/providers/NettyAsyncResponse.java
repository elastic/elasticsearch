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

import org.elasticsearch.util.http.client.*;
import org.elasticsearch.util.http.collection.Pair;
import org.elasticsearch.util.http.url.Url;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferInputStream;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.handler.codec.http.HttpResponse;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.util.*;

/**
 * Wrapper around the {@link org.elasticsearch.util.http.client.Response} API.
 */
public class NettyAsyncResponse implements Response {
    private final Url url;
    private final Collection<HttpResponseBodyPart<HttpResponse>> bodyParts;
    private final HttpResponseHeaders<HttpResponse> headers;
    private final HttpResponseStatus<HttpResponse> status;
    private final List<Cookie> cookies = new ArrayList<Cookie>();

    public NettyAsyncResponse(HttpResponseStatus<HttpResponse> status,
                              HttpResponseHeaders<HttpResponse> headers,
                              Collection<HttpResponseBodyPart<HttpResponse>> bodyParts) {

        this.status = status;
        this.headers = headers;
        this.bodyParts = bodyParts;
        url = status.getUrl();
    }

    /* @Override */

    public int getStatusCode() {
        return status.getStatusCode();
    }

    /* @Override */

    public String getStatusText() {
        return status.getStatusText();
    }

    /* @Override */

    public String getResponseBody() throws IOException {
        String contentType = getContentType();
        String charset = "UTF-8";
        if (contentType != null) {
            for (String part : contentType.split(";")) {
                if (part.startsWith("charset=")) {
                    charset = part.substring("charset=".length());
                }
            }
        }
        return contentToString(charset);
    }

    String contentToString(String charset) throws UnsupportedEncodingException {
        StringBuilder b = new StringBuilder();
        for (HttpResponseBodyPart<?> bp : bodyParts) {
            b.append(new String(bp.getBodyPartBytes(), charset));
        }
        return b.toString();
    }

    /* @Override */

    public InputStream getResponseBodyAsStream() throws IOException {
        ChannelBuffer buf = ChannelBuffers.dynamicBuffer();
        for (HttpResponseBodyPart<?> bp : bodyParts) {
            // Ugly. TODO
            // (1) We must remove the downcast,
            // (2) we need a CompositeByteArrayInputStream to avoid
            // copying the bytes.
            if (bp.getClass().isAssignableFrom(ResponseBodyPart.class)) {
                buf.writeBytes(bp.getBodyPartBytes());
            }
        }
        return new ChannelBufferInputStream(buf);
    }

    /* @Override */

    public String getResponseBodyExcerpt(int maxLength) throws IOException {
        String contentType = getContentType();
        String charset = "UTF-8";
        if (contentType != null) {
            for (String part : contentType.split(";")) {
                if (part.startsWith("charset=")) {
                    charset = part.substring("charset=".length());
                }
            }
        }
        String response = contentToString(charset);
        return response.length() <= maxLength ? response : response.substring(0, maxLength);
    }

    /* @Override */

    public Url getUrl() throws MalformedURLException {
        return url;
    }

    /* @Override */

    public String getContentType() {
        return headers.getHeaders().getHeaderValue("Content-Type");
    }

    /* @Override */

    public String getHeader(String name) {
        return headers.getHeaders().getHeaderValue(name);
    }

    /* @Override */

    public List<String> getHeaders(String name) {
        return headers.getHeaders().getHeaderValues(name);
    }

    /* @Override */

    public Headers getHeaders() {
        return headers.getHeaders();
    }

    /* @Override */

    public boolean isRedirected() {
        return (status.getStatusCode() >= 300) && (status.getStatusCode() <= 399);
    }

    /* @Override */

    public List<Cookie> getCookies() {
        if (cookies.isEmpty()) {
            Iterator<Pair<String, String>> i = headers.getHeaders().iterator();
            Pair<String, String> p;
            while (i.hasNext()) {
                p = i.next();
                if (p.getFirst().equalsIgnoreCase("Set-Cookie")) {
                    String[] fields = p.getSecond().split(";\\s*");
                    String[] cookieValue = fields[0].split("=");
                    String name = cookieValue[0];
                    String value = cookieValue[1];
                    String expires = "-1";
                    String path = null;
                    String domain = null;
                    boolean secure = false; // Parse each field
                    for (int j = 1; j < fields.length; j++) {
                        if ("secure".equalsIgnoreCase(fields[j])) {
                            secure = true;
                        } else if (fields[j].indexOf('=') > 0) {
                            String[] f = fields[j].split("=");
                            if ("expires".equalsIgnoreCase(f[0])) {
                                expires = f[1];
                            } else if ("domain".equalsIgnoreCase(f[0])) {
                                domain = f[1];
                            } else if ("path".equalsIgnoreCase(f[0])) {
                                path = f[1];
                            }
                        }
                    }
                    cookies.add(new Cookie(domain, name, value, path, Integer.valueOf(expires), secure));
                }
            }
        }
        return Collections.unmodifiableList(cookies);
    }

}

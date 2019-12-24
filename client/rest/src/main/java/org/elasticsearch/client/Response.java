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

package org.elasticsearch.client;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.RequestLine;
import org.apache.http.StatusLine;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Holds an elasticsearch response. It wraps the {@link HttpResponse} returned and associates it with
 * its corresponding {@link RequestLine} and {@link HttpHost}.
 */
public class Response {

    private final RequestLine requestLine;
    private final HttpHost host;
    private final HttpResponse response;

    Response(RequestLine requestLine, HttpHost host, HttpResponse response) {
        Objects.requireNonNull(requestLine, "requestLine cannot be null");
        Objects.requireNonNull(host, "host cannot be null");
        Objects.requireNonNull(response, "response cannot be null");
        this.requestLine = requestLine;
        this.host = host;
        this.response = response;
    }

    /**
     * Returns the request line that generated this response
     */
    public RequestLine getRequestLine() {
        return requestLine;
    }

    /**
     * Returns the node that returned this response
     */
    public HttpHost getHost() {
        return host;
    }

    /**
     * Returns the status line of the current response
     */
    public StatusLine getStatusLine() {
        return response.getStatusLine();
    }

    /**
     * Returns all the response headers
     */
    public Header[] getHeaders() {
        return response.getAllHeaders();
    }

    /**
     * Returns the value of the first header with a specified name of this message.
     * If there is more than one matching header in the message the first element is returned.
     * If there is no matching header in the message <code>null</code> is returned.
     */
    public String getHeader(String name) {
        Header header = response.getFirstHeader(name);
        if (header == null) {
            return null;
        }
        return header.getValue();
    }

    /**
     * Returns the response body available, null otherwise
     * @see HttpEntity
     */
    public HttpEntity getEntity() {
        return response.getEntity();
    }

    private static final Pattern WARNING_HEADER_PATTERN = Pattern.compile(
            "299 " + // warn code
            "Elasticsearch-" + // warn agent
            "\\d+\\.\\d+\\.\\d+(?:-(?:alpha|beta|rc)\\d+)?(?:-SNAPSHOT)?-" + // warn agent
            "(?:[a-f0-9]{7}(?:[a-f0-9]{33})?|unknown) " + // warn agent
            "\"((?:\t| |!|[\\x23-\\x5B]|[\\x5D-\\x7E]|[\\x80-\\xFF]|\\\\|\\\\\")*)\"( " + // quoted warning value, captured
            // quoted RFC 1123 date format
            "\"" + // opening quote
            "(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun), " + // weekday
            "\\d{2} " + // 2-digit day
            "(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec) " + // month
            "\\d{4} " + // 4-digit year
            "\\d{2}:\\d{2}:\\d{2} " + // (two-digit hour):(two-digit minute):(two-digit second)
            "GMT" + // GMT
            "\")?"); // closing quote (optional, since an older version can still send a warn-date)

    /**
     * Returns a list of all warning headers returned in the response.
     */
    public List<String> getWarnings() {
        List<String> warnings = new ArrayList<>();
        for (Header header : response.getHeaders("Warning")) {
            String warning = header.getValue();
            final Matcher matcher = WARNING_HEADER_PATTERN.matcher(warning);
            if (matcher.matches()) {
                warnings.add(matcher.group(1));
            } else {
                warnings.add(warning);
            }
        }
        return warnings;
    }

    /**
     * Returns true if there is at least one warning header returned in the
     * response.
     */
    public boolean hasWarnings() {
        Header[] warnings = response.getHeaders("Warning");
        return warnings != null && warnings.length > 0;
    }

    HttpResponse getHttpResponse() {
        return response;
    }

    @Override
    public String toString() {
        return "Response{" +
                "requestLine=" + requestLine +
                ", host=" + host +
                ", response=" + response.getStatusLine() +
                '}';
    }
}

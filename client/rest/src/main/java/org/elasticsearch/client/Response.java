/*
 * Licensed to Elasticsearch B.V. under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch B.V. licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

    /**
     * Optimized regular expression to test if a string matches the RFC 1123 date
     * format (with quotes and leading space). Start/end of line characters and
     * atomic groups are used to prevent backtracking.
     */
    private static final Pattern WARNING_HEADER_DATE_PATTERN = Pattern.compile("^ " + // start of line, leading space
    // quoted RFC 1123 date format
        "\"" + // opening quote
        "(?>Mon|Tue|Wed|Thu|Fri|Sat|Sun), " + // day of week, atomic group to prevent backtracking
        "\\d{2} " + // 2-digit day
        "(?>Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec) " + // month, atomic group to prevent backtracking
        "\\d{4} " + // 4-digit year
        "\\d{2}:\\d{2}:\\d{2} " + // (two-digit hour):(two-digit minute):(two-digit second)
        "GMT" + // GMT
        "\"$"); // closing quote (optional, since an older version can still send a warn-date), end of line

    /**
     * Length of RFC 1123 format (with quotes and leading space), used in
     * matchWarningHeaderPatternByPrefix(String).
     */
    // tag::noformat
    private static final int WARNING_HEADER_DATE_LENGTH = 0
            + 1
            + 1
            + 3 + 1 + 1
            + 2 + 1
            + 3 + 1
            + 4 + 1
            + 2 + 1 + 2 + 1 + 2 + 1
            + 3
            + 1;
    // end::noformat

    /**
     * Tests if a string matches the RFC 7234 specification for warning headers.
     * This assumes that the warn code is always 299 or 300 and the warn agent is
     * always Elasticsearch.
     *
     * @param s the value of a warning header formatted according to RFC 7234
     * @return {@code true} if the input string matches the specification
     */
    private static boolean matchWarningHeaderPatternByPrefix(final String s) {
        return s.startsWith("299 Elasticsearch-") || s.startsWith("300 Elasticsearch-");
    }

    /**
     * Refer to org.elasticsearch.common.logging.DeprecationLogger
     */
    private static String extractWarningValueFromWarningHeader(final String s) {
        String warningHeader = s;

        /*
         * The following block tests for the existence of a RFC 1123 date in the warning header. If the date exists, it is removed for
         * extractWarningValueFromWarningHeader(String) to work properly (as it does not handle dates).
         */
        if (s.length() > WARNING_HEADER_DATE_LENGTH) {
            final String possibleDateString = s.substring(s.length() - WARNING_HEADER_DATE_LENGTH);
            final Matcher matcher = WARNING_HEADER_DATE_PATTERN.matcher(possibleDateString);

            if (matcher.matches()) {
                warningHeader = warningHeader.substring(0, s.length() - WARNING_HEADER_DATE_LENGTH);
            }
        }

        final int firstQuote = warningHeader.indexOf('\"');
        final int lastQuote = warningHeader.length() - 1;
        final String warningValue = warningHeader.substring(firstQuote + 1, lastQuote);
        return warningValue;
    }

    /**
     * Returns a list of all warning headers returned in the response.
     */
    public List<String> getWarnings() {
        List<String> warnings = new ArrayList<>();
        for (Header header : response.getHeaders("Warning")) {
            String warning = header.getValue();
            if (matchWarningHeaderPatternByPrefix(warning)) {
                warnings.add(extractWarningValueFromWarningHeader(warning));
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
        return "Response{requestLine=" + requestLine + ", host=" + host + ", response=" + response.getStatusLine() + '}';
    }
}

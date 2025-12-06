/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest;

import com.carrotsearch.randomizedtesting.annotations.Name;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.http.HttpTransportSettings;
import org.elasticsearch.test.rest.ObjectPath;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.regex.Pattern;

import static org.elasticsearch.rest.RestStatus.BAD_REQUEST;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.matchesRegex;

public class Netty4BadRequestIT extends AbstractNetty4IT {

    public Netty4BadRequestIT(@Name("pooled") boolean pooledAllocator) {
        super(pooledAllocator);
    }

    public void testBadRequest() throws IOException {
        final Response response = client().performRequest(new Request("GET", "/_nodes/settings"));
        final ObjectPath objectPath = ObjectPath.createFromResponse(response);
        final Map<String, Object> map = objectPath.evaluate("nodes");
        int maxMaxInitialLineLength = Integer.MIN_VALUE;
        final Setting<ByteSizeValue> httpMaxInitialLineLength = HttpTransportSettings.SETTING_HTTP_MAX_INITIAL_LINE_LENGTH;
        final String key = httpMaxInitialLineLength.getKey().substring("http.".length());
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            @SuppressWarnings("unchecked")
            final Map<String, Object> settings = (Map<String, Object>) ((Map<String, Object>) entry.getValue()).get("settings");
            final int maxIntialLineLength;
            if (settings.containsKey("http")) {
                @SuppressWarnings("unchecked")
                final Map<String, Object> httpSettings = (Map<String, Object>) settings.get("http");
                if (httpSettings.containsKey(key)) {
                    maxIntialLineLength = ByteSizeValue.parseBytesSizeValue((String) httpSettings.get(key), key).bytesAsInt();
                } else {
                    maxIntialLineLength = httpMaxInitialLineLength.getDefault(Settings.EMPTY).bytesAsInt();
                }
            } else {
                maxIntialLineLength = httpMaxInitialLineLength.getDefault(Settings.EMPTY).bytesAsInt();
            }
            maxMaxInitialLineLength = Math.max(maxMaxInitialLineLength, maxIntialLineLength);
        }

        final String path = "/" + new String(new byte[maxMaxInitialLineLength], StandardCharsets.UTF_8).replace('\0', 'a');
        final ResponseException e = expectThrows(
            ResponseException.class,
            () -> client().performRequest(new Request(randomFrom("GET", "POST", "PUT"), path))
        );
        assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(BAD_REQUEST.getStatus()));
        assertThat(e, hasToString(containsString("too_long_http_line_exception")));
        assertThat(e, hasToString(matchesRegex(Pattern.compile(".*An HTTP line is larger than \\d+ bytes.*", Pattern.DOTALL))));
    }

    public void testInvalidParameterValue() throws IOException {
        final Request request = new Request("GET", "/_cluster/settings");
        request.addParameter("pretty", "neither-true-nor-false");
        final ResponseException e = expectThrows(ResponseException.class, () -> client().performRequest(request));
        final Response response = e.getResponse();
        assertThat(response.getStatusLine().getStatusCode(), equalTo(400));
        final ObjectPath objectPath = ObjectPath.createFromResponse(response);
        final Map<String, Object> map = objectPath.evaluate("error");
        assertThat(map.get("type"), equalTo("illegal_argument_exception"));
        assertThat(map.get("reason"), equalTo("Failed to parse value [neither-true-nor-false] as only [true] or [false] are allowed."));
    }

    public void testInvalidHeaderValue() throws IOException {
        final Request request = new Request("GET", "/_cluster/settings");
        final RequestOptions.Builder options = request.getOptions().toBuilder();
        options.addHeader("Content-Type", "\t");
        request.setOptions(options);
        final ResponseException e = expectThrows(ResponseException.class, () -> client().performRequest(request));
        final Response response = e.getResponse();
        assertThat(response.getStatusLine().getStatusCode(), equalTo(400));
        final ObjectPath objectPath = ObjectPath.createFromResponse(response);
        final Map<String, Object> map = objectPath.evaluate("error");
        assertThat(map.get("type"), equalTo("media_type_header_exception"));
        assertThat(map.get("reason"), equalTo("Invalid media-type value on headers [Content-Type]"));
    }
}

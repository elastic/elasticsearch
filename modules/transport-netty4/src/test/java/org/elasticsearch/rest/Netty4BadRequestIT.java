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

package org.elasticsearch.rest;

import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.http.HttpTransportSettings;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.yaml.ObjectPath;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.Map;

import static org.elasticsearch.rest.RestStatus.BAD_REQUEST;
import static org.elasticsearch.test.hamcrest.RegexMatcher.matches;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasToString;

public class Netty4BadRequestIT extends ESRestTestCase {

    public void testBadRequest() throws IOException {
        final Response response = client().performRequest("GET", "/_nodes/settings", Collections.emptyMap());
        final ObjectPath objectPath = ObjectPath.createFromResponse(response);
        final Map<String, Object> map = objectPath.evaluate("nodes");
        int maxMaxInitialLineLength = Integer.MIN_VALUE;
        final Setting<ByteSizeValue> httpMaxInitialLineLength = HttpTransportSettings.SETTING_HTTP_MAX_INITIAL_LINE_LENGTH;
        final String key = httpMaxInitialLineLength.getKey().substring("http.".length());
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            @SuppressWarnings("unchecked") final Map<String, Object> settings =
                    (Map<String, Object>)((Map<String, Object>)entry.getValue()).get("settings");
                    final int maxIntialLineLength;
            if (settings.containsKey("http")) {
                @SuppressWarnings("unchecked") final Map<String, Object> httpSettings = (Map<String, Object>)settings.get("http");
                if (httpSettings.containsKey(key)) {
                    maxIntialLineLength = ByteSizeValue.parseBytesSizeValue((String)httpSettings.get(key), key).bytesAsInt();
                } else {
                    maxIntialLineLength = httpMaxInitialLineLength.getDefault(Settings.EMPTY).bytesAsInt();
                }
            } else {
                maxIntialLineLength = httpMaxInitialLineLength.getDefault(Settings.EMPTY).bytesAsInt();
            }
            maxMaxInitialLineLength = Math.max(maxMaxInitialLineLength, maxIntialLineLength);
        }

        final String path = "/" + new String(new byte[maxMaxInitialLineLength], Charset.forName("UTF-8")).replace('\0', 'a');
        final ResponseException e =
                expectThrows(
                        ResponseException.class,
                        () -> client().performRequest(randomFrom("GET", "POST", "PUT"), path, Collections.emptyMap()));
        assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(BAD_REQUEST.getStatus()));
        assertThat(e, hasToString(containsString("too_long_frame_exception")));
        assertThat(e, hasToString(matches("An HTTP line is larger than \\d+ bytes")));
    }
}

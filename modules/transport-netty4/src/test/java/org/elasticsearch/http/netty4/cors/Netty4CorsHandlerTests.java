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

package org.elasticsearch.http.netty4.cors;

import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;

import static org.elasticsearch.http.HttpTransportSettings.SETTING_CORS_ALLOW_HEADERS;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_CORS_ALLOW_METHODS;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_CORS_ALLOW_ORIGIN;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_CORS_ENABLED;

/**
 * Tests for {@link Netty4CorsHandler}
 */
public class Netty4CorsHandlerTests extends ESTestCase {

    public void testPreflightMultiValueResponseHeaders() {
        // test when only one value
        String headersRequestHeader = "content-type";
        String methodsRequestHeader = "GET";
        Settings settings = Settings.builder()
                                .put(SETTING_CORS_ENABLED.getKey(), true)
                                .put(SETTING_CORS_ALLOW_ORIGIN.getKey(), Netty4CorsHandler.ANY_ORIGIN)
                                .put(SETTING_CORS_ALLOW_HEADERS.getKey(), headersRequestHeader)
                                .put(SETTING_CORS_ALLOW_METHODS.getKey(), methodsRequestHeader)
                                .build();
        HttpResponse response = execPreflight(settings, Netty4CorsHandler.ANY_ORIGIN, "request-host");
        assertEquals(headersRequestHeader, response.headers().get(HttpHeaderNames.ACCESS_CONTROL_ALLOW_HEADERS));
        assertEquals(methodsRequestHeader, response.headers().get(HttpHeaderNames.ACCESS_CONTROL_ALLOW_METHODS));

        // test with a set of values
        headersRequestHeader = "content-type,x-requested-with,accept";
        methodsRequestHeader = "GET,POST";
        settings = Settings.builder()
                       .put(SETTING_CORS_ENABLED.getKey(), true)
                       .put(SETTING_CORS_ALLOW_ORIGIN.getKey(), Netty4CorsHandler.ANY_ORIGIN)
                       .put(SETTING_CORS_ALLOW_HEADERS.getKey(), headersRequestHeader)
                       .put(SETTING_CORS_ALLOW_METHODS.getKey(), methodsRequestHeader)
                       .build();
        response = execPreflight(settings, Netty4CorsHandler.ANY_ORIGIN, "request-host");
        assertEquals(Strings.commaDelimitedListToSet(headersRequestHeader),
            Strings.commaDelimitedListToSet(response.headers().get(HttpHeaderNames.ACCESS_CONTROL_ALLOW_HEADERS)));
        assertEquals(Strings.commaDelimitedListToSet(methodsRequestHeader),
            Strings.commaDelimitedListToSet(response.headers().get(HttpHeaderNames.ACCESS_CONTROL_ALLOW_METHODS)));

        // test with defaults
        settings = Settings.builder()
                       .put(SETTING_CORS_ENABLED.getKey(), true)
                       .put(SETTING_CORS_ALLOW_ORIGIN.getKey(), Netty4CorsHandler.ANY_ORIGIN)
                       .build();
        response = execPreflight(settings, Netty4CorsHandler.ANY_ORIGIN, "request-host");
        assertEquals(Strings.commaDelimitedListToSet(SETTING_CORS_ALLOW_HEADERS.getDefault(settings)),
            Strings.commaDelimitedListToSet(response.headers().get(HttpHeaderNames.ACCESS_CONTROL_ALLOW_HEADERS)));
        assertEquals(Strings.commaDelimitedListToSet(SETTING_CORS_ALLOW_METHODS.getDefault(settings)),
            Strings.commaDelimitedListToSet(response.headers().get(HttpHeaderNames.ACCESS_CONTROL_ALLOW_METHODS)));
    }

    private HttpResponse execPreflight(final Settings settings, final String originValue, final String host) {
        // simulate execution of a preflight request
        final FullHttpRequest httpRequest = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/");
        httpRequest.setMethod(HttpMethod.OPTIONS);
        httpRequest.headers().add(HttpHeaderNames.ORIGIN, originValue);
        httpRequest.headers().add(HttpHeaderNames.HOST, host);
        httpRequest.headers().add(HttpHeaderNames.ACCESS_CONTROL_REQUEST_METHOD, "GET");

        Netty4CorsHandler corsHandler = new Netty4CorsHandler(Netty4CorsConfig.buildCorsConfig(settings));
        return corsHandler.handlePreflight(httpRequest);
    }
}

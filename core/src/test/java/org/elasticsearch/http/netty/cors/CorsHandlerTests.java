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

package org.elasticsearch.http.netty.cors;

import org.elasticsearch.common.Strings;
import org.elasticsearch.test.ESTestCase;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpVersion;
import org.junit.Test;

import java.util.Set;

import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.ACCESS_CONTROL_ALLOW_HEADERS;
import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.ACCESS_CONTROL_ALLOW_METHODS;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.OK;

/**
 * Tests for {@link CorsHandler}
 */
public class CorsHandlerTests extends ESTestCase {

    @Test
    public void testSingleValueResponseHeaders() {
        CorsConfig corsConfig = new CorsConfigBuilder()
                                    .allowedRequestHeaders("content-type")
                                    .allowedRequestMethods(HttpMethod.GET)
                                    .build();
        CorsHandler corsHandler = new CorsHandler(corsConfig);
        HttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, OK);
        corsHandler.setAllowMethods(response);
        corsHandler.setAllowHeaders(response);
        assertEquals("content-type", response.headers().get(ACCESS_CONTROL_ALLOW_HEADERS));
        assertEquals("GET", response.headers().get(ACCESS_CONTROL_ALLOW_METHODS));
    }

    @Test
    public void testMultiValueResponseHeaders() {
        CorsConfig corsConfig = new CorsConfigBuilder()
                                    .allowedRequestHeaders("content-type,x-requested-with,accept")
                                    .allowedRequestMethods(HttpMethod.GET, HttpMethod.POST)
                                    .build();
        CorsHandler corsHandler = new CorsHandler(corsConfig);
        HttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, OK);
        corsHandler.setAllowMethods(response);
        corsHandler.setAllowHeaders(response);
        Set<String> responseHeadersSet = Strings.commaDelimitedListToSet(response.headers().get(ACCESS_CONTROL_ALLOW_HEADERS));
        assertEquals(3, responseHeadersSet.size());
        assertTrue(responseHeadersSet.contains("content-type"));
        assertTrue(responseHeadersSet.contains("x-requested-with"));
        assertTrue(responseHeadersSet.contains("accept"));
        Set<String> responseMethodsSet = Strings.commaDelimitedListToSet(response.headers().get(ACCESS_CONTROL_ALLOW_METHODS));
        assertEquals(2, responseMethodsSet.size());
        assertTrue(responseMethodsSet.contains("GET"));
        assertTrue(responseMethodsSet.contains("POST"));
    }
}

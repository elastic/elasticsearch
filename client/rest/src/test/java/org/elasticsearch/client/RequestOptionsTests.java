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
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.nio.entity.NStringEntity;
import org.elasticsearch.client.HttpAsyncResponseConsumerFactory.HeapBufferedResponseConsumerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

public class RequestOptionsTests extends RestClientTestCase {
    public void testDefault() {
        assertEquals(Collections.<Header>emptyList(), RequestOptions.DEFAULT.getHeaders());
        assertEquals(HttpAsyncResponseConsumerFactory.DEFAULT, RequestOptions.DEFAULT.getHttpAsyncResponseConsumerFactory());
        assertEquals(RequestOptions.DEFAULT, RequestOptions.DEFAULT.toBuilder().build());
    }

    public void testAddHeader() {
        try {
            RequestOptions.DEFAULT.toBuilder().addHeader(null, randomAsciiLettersOfLengthBetween(3, 10));
            fail("expected failure");
        } catch (NullPointerException e) {
            assertEquals("header name cannot be null", e.getMessage());
        }

        try {
            RequestOptions.DEFAULT.toBuilder().addHeader(randomAsciiLettersOfLengthBetween(3, 10), null);
            fail("expected failure");
        } catch (NullPointerException e) {
            assertEquals("header value cannot be null", e.getMessage());
        }

        RequestOptions.Builder builder = RequestOptions.DEFAULT.toBuilder();
        int numHeaders = between(0, 5);
        List<Header> headers = new ArrayList<>();
        for (int i = 0; i < numHeaders; i++) {
            Header header = new RequestOptions.ReqHeader(randomAsciiAlphanumOfLengthBetween(5, 10), randomAsciiAlphanumOfLength(3));
            headers.add(header);
            builder.addHeader(header.getName(), header.getValue());
        }
        RequestOptions options = builder.build();
        assertEquals(headers, options.getHeaders());

        try {
            options.getHeaders().add(
                    new RequestOptions.ReqHeader(randomAsciiAlphanumOfLengthBetween(5, 10), randomAsciiAlphanumOfLength(3)));
            fail("expected failure");
        } catch (UnsupportedOperationException e) {
            assertEquals("header value cannot be null", e.getMessage());
        }
    }

    public void testSetHttpAsyncResponseConsumerFactory() {
        try {
            RequestOptions.DEFAULT.toBuilder().setHttpAsyncResponseConsumerFactory(null);
            fail("expected failure");
        } catch (NullPointerException e) {
            assertEquals("httpAsyncResponseConsumerFactory cannot be null", e.getMessage());
        }

        HttpAsyncResponseConsumerFactory factory = mock(HttpAsyncResponseConsumerFactory.class);
        RequestOptions.Builder builder = RequestOptions.DEFAULT.toBuilder();
        builder.setHttpAsyncResponseConsumerFactory(factory);
        RequestOptions options = builder.build();
        assertSame(factory, options.getHttpAsyncResponseConsumerFactory());
    }

    public void testEqualsAndHashCode() {
        RequestOptions request = randomRequestOptions();
        assertEquals(request, request);

        Request copy = copy(request);
        assertEquals(request, copy);
        assertEquals(copy, request);
        assertEquals(request.hashCode(), copy.hashCode());

        Request mutant = mutate(request);
        assertNotEquals(request, mutant);
        assertNotEquals(mutant, request);
    }

    private static RequestOptions randomRequestOptions() {
        RequestOptions.Builder builder = RequestOptions.DEFAULT.toBuilder();

        if (randomBoolean()) {
            int headerCount = between(1, 5);
            for (int i = 0; i < headerCount; i++) {
                builder.addHeader(randomAsciiAlphanumOfLength(3), randomAsciiAlphanumOfLength(3));
            }
        }

        if (randomBoolean()) {
            builder.setHttpAsyncResponseConsumerFactory(new HeapBufferedResponseConsumerFactory(1));
        }

        return builder.build();
    }

    private static RequestOptions copy(RequestOptions options) {
        return request.toBuilder().build();
    }

    private static RequestOptions mutate(RequestOptions options) {
        RequestOptions.Builder mutant = options.builder();
        int mutationType = between(0, 3);
        switch (mutationType) {
        case 0:
            mutant.addParameter(randomAsciiAlphanumOfLength(mutant.getParameters().size() + 4), "extra");
            return mutant;
        case 1:
            mutant.setJsonEntity("mutant"); // randomRequest can't produce this value
            return mutant;
        case 2:
            mutant.addHeader("extra", "m");
            return mutant;
        case 3:
            mutant.setHttpAsyncResponseConsumerFactory(new HeapBufferedResponseConsumerFactory(5));
            return mutant;
        default:
            throw new UnsupportedOperationException("Unknown mutation type [" + mutationType + "]");
        }
    }

    private static void copyMutables(Request from, Request to) {
        for (Map.Entry<String, String> param : from.getParameters().entrySet()) {
            to.addParameter(param.getKey(), param.getValue());
        }
        to.setEntity(from.getEntity());
        for (Header header : from.getHeaders()) {
            to.addHeader(header.getName(), header.getValue());
        }
        to.setHttpAsyncResponseConsumerFactory(from.getHttpAsyncResponseConsumerFactory());
    }
}

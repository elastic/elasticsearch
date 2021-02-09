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

import org.apache.http.HttpEntity;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.nio.entity.NStringEntity;
import org.elasticsearch.client.HttpAsyncResponseConsumerFactory.HeapBufferedResponseConsumerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

public class RequestTests extends RestClientTestCase {
    public void testConstructor() {
        final String method = randomFrom(new String[] {"GET", "PUT", "POST", "HEAD", "DELETE"});
        final String endpoint = randomAsciiLettersOfLengthBetween(1, 10);

        try {
            new Request(null, endpoint);
            fail("expected failure");
        } catch (NullPointerException e) {
            assertEquals("method cannot be null", e.getMessage());
        }

        try {
            new Request(method, null);
            fail("expected failure");
        } catch (NullPointerException e) {
            assertEquals("endpoint cannot be null", e.getMessage());
        }

        final Request request = new Request(method, endpoint);
        assertEquals(method, request.getMethod());
        assertEquals(endpoint, request.getEndpoint());
    }

    public void testAddParameters() {
        final String method = randomFrom(new String[] {"GET", "PUT", "POST", "HEAD", "DELETE"});
        final String endpoint = randomAsciiLettersOfLengthBetween(1, 10);
        int parametersCount = between(1, 3);
        final Map<String, String> parameters = new HashMap<>(parametersCount);
        while (parameters.size() < parametersCount) {
            parameters.put(randomAsciiLettersOfLength(5), randomAsciiLettersOfLength(5));
        }
        Request request = new Request(method, endpoint);

        try {
            request.addParameter(null, "value");
            fail("expected failure");
        } catch (NullPointerException e) {
            assertEquals("url parameter name cannot be null", e.getMessage());
        }

        for (Map.Entry<String, String> entry : parameters.entrySet()) {
            request.addParameter(entry.getKey(), entry.getValue());
        }
        assertEquals(parameters, request.getParameters());

        // Test that adding parameters with a null value is ok.
        request.addParameter("is_null", null);
        parameters.put("is_null", null);
        assertEquals(parameters, request.getParameters());

        // Test that adding a duplicate parameter fails
        String firstValue = randomBoolean() ? null : "value";
        request.addParameter("name", firstValue);
        try {
            request.addParameter("name", randomBoolean() ? firstValue : "second_value");
            fail("expected failure");
        } catch (IllegalArgumentException e) {
            assertEquals("url parameter [name] has already been set to [" + firstValue + "]", e.getMessage());
        }
    }

    public void testSetEntity() {
        final String method = randomFrom(new String[] {"GET", "PUT", "POST", "HEAD", "DELETE"});
        final String endpoint = randomAsciiLettersOfLengthBetween(1, 10);
        final HttpEntity entity =
                randomBoolean() ? new StringEntity(randomAsciiLettersOfLengthBetween(1, 100), ContentType.TEXT_PLAIN) : null;

        Request request = new Request(method, endpoint);
        request.setEntity(entity);
        assertEquals(entity, request.getEntity());
    }

    public void testSetJsonEntity() throws IOException {
        final String method = randomFrom(new String[] {"GET", "PUT", "POST", "HEAD", "DELETE"});
        final String endpoint = randomAsciiLettersOfLengthBetween(1, 10);

        Request request = new Request(method, endpoint);
        assertNull(request.getEntity());

        final String json = randomAsciiLettersOfLengthBetween(1, 100);
        request.setJsonEntity(json);
        assertEquals(ContentType.APPLICATION_JSON.toString(), request.getEntity().getContentType().getValue());
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        request.getEntity().writeTo(os);
        assertEquals(json, new String(os.toByteArray(), ContentType.APPLICATION_JSON.getCharset()));
    }

    public void testSetOptions() {
        final String method = randomFrom(new String[] {"GET", "PUT", "POST", "HEAD", "DELETE"});
        final String endpoint = randomAsciiLettersOfLengthBetween(1, 10);
        Request request = new Request(method, endpoint);

        try {
            request.setOptions((RequestOptions) null);
            fail("expected failure");
        } catch (NullPointerException e) {
            assertEquals("options cannot be null", e.getMessage());
        }

        try {
            request.setOptions((RequestOptions.Builder) null);
            fail("expected failure");
        } catch (NullPointerException e) {
            assertEquals("options cannot be null", e.getMessage());
        }

        RequestOptions.Builder builder = RequestOptionsTests.randomBuilder();
        request.setOptions(builder);
        assertEquals(builder.build(), request.getOptions());

        builder = RequestOptionsTests.randomBuilder();
        RequestOptions options = builder.build();
        request.setOptions(options);
        assertSame(options, request.getOptions());
    }

    public void testEqualsAndHashCode() {
        Request request = randomRequest();
        assertEquals(request, request);

        Request copy = copy(request);
        assertEquals(request, copy);
        assertEquals(copy, request);
        assertEquals(request.hashCode(), copy.hashCode());

        Request mutant = mutate(request);
        assertNotEquals(request, mutant);
        assertNotEquals(mutant, request);
    }

    private static Request randomRequest() {
        Request request = new Request(
            randomFrom(new String[] {"GET", "PUT", "DELETE", "POST", "HEAD", "OPTIONS"}),
            randomAsciiAlphanumOfLength(5));

        int parameterCount = between(0, 5);
        for (int i = 0; i < parameterCount; i++) {
            request.addParameter(randomAsciiAlphanumOfLength(i), randomAsciiLettersOfLength(3));
        }

        if (randomBoolean()) {
            if (randomBoolean()) {
                request.setJsonEntity(randomAsciiAlphanumOfLength(10));
            } else {
                request.setEntity(randomFrom(new HttpEntity[] {
                    new StringEntity(randomAsciiAlphanumOfLength(10), ContentType.APPLICATION_JSON),
                    new NStringEntity(randomAsciiAlphanumOfLength(10), ContentType.APPLICATION_JSON),
                    new ByteArrayEntity(randomBytesOfLength(40), ContentType.APPLICATION_JSON)
                }));
            }
        }

        if (randomBoolean()) {
            RequestOptions.Builder options = request.getOptions().toBuilder();
            options.setHttpAsyncResponseConsumerFactory(new HeapBufferedResponseConsumerFactory(1));
            request.setOptions(options);
        }

        return request;
    }

    private static Request copy(Request request) {
        Request copy = new Request(request.getMethod(), request.getEndpoint());
        copyMutables(request, copy);
        return copy;
    }

    private static Request mutate(Request request) {
        if (randomBoolean()) {
            // Mutate request or method but keep everything else constant
            Request mutant = randomBoolean()
                ? new Request(request.getMethod() + "m", request.getEndpoint())
                : new Request(request.getMethod(), request.getEndpoint() + "m");
            copyMutables(request, mutant);
            return mutant;
        }
        Request mutant = copy(request);
        int mutationType = between(0, 2);
        switch (mutationType) {
        case 0:
            mutant.addParameter(randomAsciiAlphanumOfLength(mutant.getParameters().size() + 4), "extra");
            return mutant;
        case 1:
            mutant.setJsonEntity("mutant"); // randomRequest can't produce this value
            return mutant;
        case 2:
            RequestOptions.Builder options = mutant.getOptions().toBuilder();
            options.addHeader("extra", "m");
            mutant.setOptions(options);
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
        to.setOptions(from.getOptions());
    }
}

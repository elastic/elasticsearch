/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.rest.yaml;

import org.apache.http.HttpEntity;
import org.elasticsearch.Version;
import org.elasticsearch.client.NodeSelector;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.is;

public class ClientYamlTestExecutionContextTests extends ESTestCase {

    public void testHeadersSupportStashedValueReplacement() throws IOException {
        final AtomicReference<Map<String, String>> headersRef = new AtomicReference<>();
        final Version version = VersionUtils.randomVersion(random());
        final ClientYamlTestExecutionContext context = new ClientYamlTestExecutionContext(null, null, randomBoolean()) {
            @Override
            ClientYamlTestResponse callApiInternal(
                String apiName,
                Map<String, String> params,
                HttpEntity entity,
                Map<String, String> headers,
                NodeSelector nodeSelector
            ) {
                headersRef.set(headers);
                return null;
            }

            @Override
            public Version esVersion() {
                return version;
            }
        };
        final Map<String, String> headers = new HashMap<>();
        headers.put("foo", "$bar");
        headers.put("foo1", "baz ${c}");

        context.stash().stashValue("bar", "foo2");
        context.stash().stashValue("c", "bar1");

        assertNull(headersRef.get());
        context.callApi("test", Collections.emptyMap(), Collections.emptyList(), headers);
        assertNotNull(headersRef.get());
        assertNotEquals(headers, headersRef.get());

        assertEquals("foo2", headersRef.get().get("foo"));
        assertEquals("baz bar1", headersRef.get().get("foo1"));
    }

    public void testStashHeadersOnException() throws IOException {
        final Version version = VersionUtils.randomVersion(random());
        final ClientYamlTestExecutionContext context = new ClientYamlTestExecutionContext(null, null, randomBoolean()) {
            @Override
            ClientYamlTestResponse callApiInternal(
                String apiName,
                Map<String, String> params,
                HttpEntity entity,
                Map<String, String> headers,
                NodeSelector nodeSelector
            ) {
                throw new RuntimeException("boom!");
            }

            @Override
            public Version esVersion() {
                return version;
            }
        };
        final Map<String, String> headers = new HashMap<>();
        headers.put("Accept", "application/json");
        headers.put("Authorization", "Basic password==");
        try {
            context.callApi("test", Collections.emptyMap(), Collections.emptyList(), headers);
        } catch (Exception e) {
            // do nothing...behavior we are testing is the finally block of the production code
        }
        assertThat(context.stash().getValue("$request_headers"), is(headers));
    }
}

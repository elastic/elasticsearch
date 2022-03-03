/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client;

import joptsimple.internal.Strings;

import org.apache.http.Header;
import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;
import java.util.Map;

/**
 * A test case with access to internals of a RestClient.
 */
public abstract class RestClientBuilderTestCase extends ESTestCase {
    /** Checks the given rest client has the provided default headers. */
    public void assertHeaders(RestClient client, Map<String, String> expectedHeaders) {
        expectedHeaders = new HashMap<>(expectedHeaders); // copy so we can remove as we check
        for (Header header : client.defaultHeaders) {
            String name = header.getName();
            String expectedValue = expectedHeaders.remove(name);
            if (expectedValue == null) {
                fail("Found unexpected header in rest client: " + name);
            }
            assertEquals(expectedValue, header.getValue());
        }
        if (expectedHeaders.isEmpty() == false) {
            fail("Missing expected headers in rest client: " + Strings.join(expectedHeaders.keySet(), ", "));
        }
    }
}

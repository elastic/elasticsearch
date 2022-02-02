/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client;

import org.apache.http.client.methods.HttpGet;
import org.elasticsearch.client.migration.DeprecationInfoRequest;
import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;
import java.util.Map;

public class MigrationRequestConvertersTests extends ESTestCase {

    public void testGetDeprecationInfo() {
        DeprecationInfoRequest deprecationInfoRequest = new DeprecationInfoRequest();
        String expectedEndpoint = "/_migration/deprecations";

        Map<String, String> expectedParams = new HashMap<>();
        Request request = MigrationRequestConverters.getDeprecationInfo(deprecationInfoRequest);
        assertEquals(HttpGet.METHOD_NAME, request.getMethod());
        assertEquals(expectedEndpoint, request.getEndpoint());
        assertNull(request.getEntity());
        assertEquals(expectedParams, request.getParameters());
    }

}

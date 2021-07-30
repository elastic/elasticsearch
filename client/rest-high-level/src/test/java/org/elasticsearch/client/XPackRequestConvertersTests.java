/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client;

import org.apache.http.client.methods.HttpGet;
import org.elasticsearch.client.xpack.XPackInfoRequest;
import org.elasticsearch.test.ESTestCase;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

public class XPackRequestConvertersTests extends ESTestCase {

    public void testXPackInfo() {
        XPackInfoRequest infoRequest = new XPackInfoRequest();
        Map<String, String> expectedParams = new HashMap<>();
        infoRequest.setVerbose(ESTestCase.randomBoolean());
        if (false == infoRequest.isVerbose()) {
            expectedParams.put("human", "false");
        }
        int option = ESTestCase.between(0, 2);
        switch (option) {
        case 0:
            infoRequest.setCategories(EnumSet.allOf(XPackInfoRequest.Category.class));
            break;
        case 1:
            infoRequest.setCategories(EnumSet.of(XPackInfoRequest.Category.FEATURES));
            expectedParams.put("categories", "features");
            break;
        case 2:
            infoRequest.setCategories(EnumSet.of(XPackInfoRequest.Category.FEATURES, XPackInfoRequest.Category.BUILD));
            expectedParams.put("categories", "build,features");
            break;
        default:
            throw new IllegalArgumentException("invalid option [" + option + "]");
        }

        Request request = XPackRequestConverters.info(infoRequest);
        assertEquals(HttpGet.METHOD_NAME, request.getMethod());
        assertEquals("/_xpack", request.getEndpoint());
        assertNull(request.getEntity());
        assertEquals(expectedParams, request.getParameters());
    }
}

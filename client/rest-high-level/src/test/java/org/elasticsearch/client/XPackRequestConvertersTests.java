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

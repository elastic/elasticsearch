/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
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

package org.elasticsearch.rest.action.document;

import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.test.rest.RestActionTestCase;
import org.junit.Before;

public class RestDeleteActionV7Tests extends RestActionTestCase {
    @Before
    public void setUpAction() {
        controller().registerHandler(new RestDeleteActionV7());
        controller().registerHandler(new RestDeleteAction());
    }

    public void testTypeInPath() {
        RestRequest deprecatedRequest = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.DELETE)
            .withPath("/some_index/some_type/some_id")
            .build();
        dispatchRequest(deprecatedRequest);
        assertWarnings(RestDeleteActionV7.TYPES_DEPRECATION_MESSAGE);

        RestRequest validRequest = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.DELETE)
            .withPath("/some_index/_doc/some_id")
            .build();
        dispatchRequest(validRequest);
    }
}

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

package org.elasticsearch.rest.action.document;

import org.elasticsearch.compat.FakeCompatRestRequestBuilder;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.test.rest.RestActionTestCase;
import org.junit.Before;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

public class RestIndexActionV7Tests extends RestActionTestCase {

    final String mimeType = "application/vnd.elasticsearch+json;compatible-with=7";
    final List<String> contentTypeHeader = Collections.singletonList(mimeType);

    private final AtomicReference<ClusterState> clusterStateSupplier = new AtomicReference<>();

    @Before
    public void setUpAction() {
        controller().registerHandler(new RestIndexActionV7.CompatibleRestIndexAction());
        controller().registerHandler(new RestIndexActionV7.CompatibleCreateHandler());
        controller().registerHandler(new RestIndexActionV7.CompatibleAutoIdHandler(() -> clusterStateSupplier.get().nodes()));
    }

    public void testTypeInPath() {
        // using CompatibleRestIndexAction
        RestRequest deprecatedRequest = new FakeCompatRestRequestBuilder(xContentRegistry()).withMethod(RestRequest.Method.PUT)
            .withPath("/some_index/some_type/some_id")
            .build();
        dispatchRequest(deprecatedRequest);
        assertWarnings(RestIndexActionV7.TYPES_DEPRECATION_MESSAGE);
    }

    public void testCreateWithTypeInPath() {
        // using CompatibleCreateHandler
        RestRequest deprecatedRequest = new FakeCompatRestRequestBuilder(xContentRegistry()).withMethod(RestRequest.Method.PUT)
            .withPath("/some_index/some_type/some_id/_create")
            .build();
        dispatchRequest(deprecatedRequest);
        assertWarnings(RestIndexActionV7.TYPES_DEPRECATION_MESSAGE);
    }

    public void testAutoIdWithType() {
        // using CompatibleAutoIdHandler
        RestRequest deprecatedRequest = new FakeCompatRestRequestBuilder(xContentRegistry()).withMethod(RestRequest.Method.POST)
            .withPath("/some_index/some_type/")
            .build();
        dispatchRequest(deprecatedRequest);
        assertWarnings(RestIndexActionV7.TYPES_DEPRECATION_MESSAGE);
    }
}

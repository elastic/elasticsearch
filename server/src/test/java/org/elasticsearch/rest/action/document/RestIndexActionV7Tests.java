/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.document;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.compatibility.RestApiCompatibleVersion;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.test.rest.RestActionTestCase;
import org.junit.Before;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.instanceOf;

public class RestIndexActionV7Tests extends RestActionTestCase {

    final List<String> contentTypeHeader =
        Collections.singletonList("application/vnd.elasticsearch+json;compatible-with="+ RestApiCompatibleVersion.V_7.major);


    private final AtomicReference<ClusterState> clusterStateSupplier = new AtomicReference<>();

    @Before
    public void setUpAction() {
        controller().registerHandler(new RestIndexActionV7.CompatibleRestIndexAction());
        controller().registerHandler(new RestIndexActionV7.CompatibleCreateHandler());
        controller().registerHandler(new RestIndexActionV7.CompatibleAutoIdHandler(() -> clusterStateSupplier.get().nodes()));

        verifyingClient.setExecuteVerifier((actionType, request) -> {
            assertThat(request, instanceOf(IndexRequest.class));
        });
    }

    public void testTypeInPath() {
        // using CompatibleRestIndexAction
        RestRequest deprecatedRequest = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.PUT)
            .withHeaders(Map.of("Content-Type", contentTypeHeader, "Accept", contentTypeHeader))
            .withPath("/some_index/some_type/some_id")
            .build();
        dispatchRequest(deprecatedRequest);
        assertWarnings(RestIndexActionV7.TYPES_DEPRECATION_MESSAGE, RestIndexActionV7.COMPATIBLE_API_MESSAGE);
    }

    public void testCreateWithTypeInPath() {
        // using CompatibleCreateHandler
        RestRequest deprecatedRequest = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.PUT)
            .withHeaders(Map.of("Content-Type", contentTypeHeader, "Accept", contentTypeHeader))
            .withPath("/some_index/some_type/some_id/_create")
            .build();
        dispatchRequest(deprecatedRequest);
        assertWarnings(RestIndexActionV7.TYPES_DEPRECATION_MESSAGE, RestIndexActionV7.COMPATIBLE_API_MESSAGE);
    }

    public void testAutoIdWithType() {
        // using CompatibleAutoIdHandler
        RestRequest deprecatedRequest = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
            .withHeaders(Map.of("Content-Type", contentTypeHeader, "Accept", contentTypeHeader))
            .withPath("/some_index/some_type/")
            .build();
        dispatchRequest(deprecatedRequest);
        assertWarnings(RestIndexActionV7.TYPES_DEPRECATION_MESSAGE, RestIndexActionV7.COMPATIBLE_API_MESSAGE);
    }
}

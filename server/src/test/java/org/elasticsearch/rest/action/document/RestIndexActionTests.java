/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.document;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.Version;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.document.RestIndexAction.AutoIdHandler;
import org.elasticsearch.rest.action.document.RestIndexAction.CreateHandler;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.test.rest.RestActionTestCase;
import org.junit.Before;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class RestIndexActionTests extends RestActionTestCase {

    final List<String> contentTypeHeader = Collections.singletonList(randomCompatibleMediaType(RestApiVersion.V_7));

    private final AtomicReference<ClusterState> clusterStateSupplier = new AtomicReference<>();

    @Before
    public void setUpAction() {
        controller().registerHandler(new RestIndexAction());
        controller().registerHandler(new CreateHandler());
        controller().registerHandler(new AutoIdHandler(() -> clusterStateSupplier.get().nodes()));
    }

    public void testCreateOpTypeValidation() {
        RestIndexAction.CreateHandler create = new CreateHandler();

        String opType = randomFrom("CREATE", null);
        create.validateOpType(opType);

        String illegalOpType = randomFrom("index", "unknown", "");
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> create.validateOpType(illegalOpType));
        assertThat(e.getMessage(), equalTo("opType must be 'create', found: [" + illegalOpType + "]"));
    }

    public void testAutoIdDefaultsToOptypeCreate() {
        checkAutoIdOpType(Version.CURRENT, DocWriteRequest.OpType.CREATE);
    }

    public void testAutoIdDefaultsToOptypeIndexForOlderVersions() {
        checkAutoIdOpType(VersionUtils.randomVersionBetween(random(), null,
            VersionUtils.getPreviousVersion(Version.V_7_5_0)), DocWriteRequest.OpType.INDEX);
    }

    private void checkAutoIdOpType(Version minClusterVersion, DocWriteRequest.OpType expectedOpType) {
        SetOnce<Boolean> executeCalled = new SetOnce<>();
        verifyingClient.setExecuteVerifier((actionType, request) -> {
            assertThat(request, instanceOf(IndexRequest.class));
            assertThat(((IndexRequest) request).opType(), equalTo(expectedOpType));
            executeCalled.set(true);
            return new IndexResponse(new ShardId("test", "test", 0), "id", 0, 0, 0, true);
        });
        RestRequest autoIdRequest = new FakeRestRequest.Builder(xContentRegistry())
            .withMethod(RestRequest.Method.POST)
            .withPath("/some_index/_doc")
            .withContent(new BytesArray("{}"), XContentType.JSON)
            .build();
        clusterStateSupplier.set(ClusterState.builder(ClusterName.DEFAULT)
            .nodes(DiscoveryNodes.builder()
                .add(new DiscoveryNode("test", buildNewFakeTransportAddress(), minClusterVersion))
                .build()).build());
        dispatchRequest(autoIdRequest);
        assertThat(executeCalled.get(), equalTo(true));
    }

    public void testTypeInPath() {
        // using CompatibleRestIndexAction
        RestRequest deprecatedRequest = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.PUT)
            .withHeaders(Map.of("Content-Type", contentTypeHeader, "Accept", contentTypeHeader))
            .withPath("/some_index/some_type/some_id")
            .build();
        dispatchRequest(deprecatedRequest);
        assertWarnings(RestIndexAction.TYPES_DEPRECATION_MESSAGE);
    }

    public void testCreateWithTypeInPath() {
        // using CompatibleCreateHandler
        RestRequest deprecatedRequest = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.PUT)
            .withHeaders(Map.of("Content-Type", contentTypeHeader, "Accept", contentTypeHeader))
            .withPath("/some_index/some_type/some_id/_create")
            .build();
        dispatchRequest(deprecatedRequest);
        assertWarnings(RestIndexAction.TYPES_DEPRECATION_MESSAGE);
    }

    public void testAutoIdWithType() {
        // using CompatibleAutoIdHandler
        RestRequest deprecatedRequest = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
            .withHeaders(Map.of("Content-Type", contentTypeHeader, "Accept", contentTypeHeader))
            .withPath("/some_index/some_type/")
            .build();
        dispatchRequest(deprecatedRequest);
        assertWarnings(RestIndexAction.TYPES_DEPRECATION_MESSAGE);
    }
}

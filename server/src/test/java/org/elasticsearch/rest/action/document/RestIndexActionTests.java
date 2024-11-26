/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest.action.document;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.document.RestIndexAction.AutoIdHandler;
import org.elasticsearch.rest.action.document.RestIndexAction.CreateHandler;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.test.rest.RestActionTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.junit.Before;

import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public final class RestIndexActionTests extends RestActionTestCase {
    private final AtomicReference<ClusterState> clusterStateSupplier = new AtomicReference<>();

    @Before
    public void setUpAction() {
        controller().registerHandler(new RestIndexAction());
        controller().registerHandler(new CreateHandler());
        controller().registerHandler(new AutoIdHandler());
    }

    public void testCreateOpTypeValidation() {
        RestIndexAction.CreateHandler create = new CreateHandler();

        String opType = randomFrom("CREATE", null);
        CreateHandler.validateOpType(opType);

        String illegalOpType = randomFrom("index", "unknown", "");
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> CreateHandler.validateOpType(illegalOpType));
        assertThat(e.getMessage(), equalTo("opType must be 'create', found: [" + illegalOpType + "]"));
    }

    public void testAutoIdDefaultsToOptypeCreate() {
        checkAutoIdOpType(DocWriteRequest.OpType.CREATE);
    }

    private void checkAutoIdOpType(DocWriteRequest.OpType expectedOpType) {
        SetOnce<Boolean> executeCalled = new SetOnce<>();
        verifyingClient.setExecuteVerifier((actionType, request) -> {
            assertThat(request, instanceOf(IndexRequest.class));
            assertThat(((IndexRequest) request).opType(), equalTo(expectedOpType));
            executeCalled.set(true);
            return new IndexResponse(new ShardId("test", "test", 0), "id", 0, 0, 0, true);
        });
        RestRequest autoIdRequest = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
            .withPath("/some_index/_doc")
            .withContent(new BytesArray("{}"), XContentType.JSON)
            .build();
        clusterStateSupplier.set(
            ClusterState.builder(ClusterName.DEFAULT).nodes(DiscoveryNodes.builder().add(DiscoveryNodeUtils.create("test")).build()).build()
        );
        dispatchRequest(autoIdRequest);
        assertThat(executeCalled.get(), equalTo(true));
    }
}

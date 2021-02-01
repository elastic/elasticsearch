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

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.Version;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.document.RestIndexAction.AutoIdHandler;
import org.elasticsearch.rest.action.document.RestIndexAction.CreateHandler;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.test.rest.RestActionTestCase;
import org.junit.Before;

import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class RestIndexActionTests extends RestActionTestCase {

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
}

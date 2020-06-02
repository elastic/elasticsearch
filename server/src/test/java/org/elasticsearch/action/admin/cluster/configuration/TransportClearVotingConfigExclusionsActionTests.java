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
package org.elasticsearch.action.admin.cluster.configuration;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.Version;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.coordination.CoordinationMetadata;
import org.elasticsearch.cluster.coordination.CoordinationMetadata.VotingConfigExclusion;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.node.DiscoveryNodes.Builder;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.transport.MockTransport;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.threadpool.ThreadPool.Names;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.elasticsearch.cluster.ClusterState.builder;
import static org.elasticsearch.test.ClusterServiceUtils.createClusterService;
import static org.elasticsearch.test.ClusterServiceUtils.setState;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.startsWith;

public class TransportClearVotingConfigExclusionsActionTests extends ESTestCase {

    private static ThreadPool threadPool;
    private static ClusterService clusterService;
    private static DiscoveryNode localNode, otherNode1, otherNode2;
    private static VotingConfigExclusion otherNode1Exclusion, otherNode2Exclusion;

    private TransportService transportService;

    @BeforeClass
    public static void createThreadPoolAndClusterService() {
        threadPool = new TestThreadPool("test", Settings.EMPTY);
        localNode = new DiscoveryNode("local", buildNewFakeTransportAddress(), Version.CURRENT);
        otherNode1 = new DiscoveryNode("other1", "other1", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT);
        otherNode1Exclusion = new VotingConfigExclusion(otherNode1);
        otherNode2 = new DiscoveryNode("other2", "other2", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT);
        otherNode2Exclusion = new VotingConfigExclusion(otherNode2);
        clusterService = createClusterService(threadPool, localNode);
    }

    @AfterClass
    public static void shutdownThreadPoolAndClusterService() {
        clusterService.stop();
        threadPool.shutdown();
    }

    @Before
    public void setupForTest() {
        final MockTransport transport = new MockTransport();
        transportService = transport.createTransportService(Settings.EMPTY, threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR, boundTransportAddress -> localNode, null, emptySet());

        new TransportClearVotingConfigExclusionsAction(transportService, clusterService, threadPool, new ActionFilters(emptySet()),
            new IndexNameExpressionResolver()); // registers action

        transportService.start();
        transportService.acceptIncomingRequests();

        final ClusterState.Builder builder = builder(new ClusterName("cluster"))
            .nodes(new Builder().add(localNode).add(otherNode1).add(otherNode2)
                .localNodeId(localNode.getId()).masterNodeId(localNode.getId()));
        builder.metadata(Metadata.builder()
                .coordinationMetadata(CoordinationMetadata.builder()
                        .addVotingConfigExclusion(otherNode1Exclusion)
                        .addVotingConfigExclusion(otherNode2Exclusion)
                .build()));
        setState(clusterService, builder);
    }

    public void testClearsVotingConfigExclusions() throws InterruptedException {
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        final SetOnce<ClearVotingConfigExclusionsResponse> responseHolder = new SetOnce<>();

        final ClearVotingConfigExclusionsRequest clearVotingConfigExclusionsRequest = new ClearVotingConfigExclusionsRequest();
        clearVotingConfigExclusionsRequest.setWaitForRemoval(false);
        transportService.sendRequest(localNode, ClearVotingConfigExclusionsAction.NAME,
            clearVotingConfigExclusionsRequest,
            expectSuccess(r -> {
                responseHolder.set(r);
                countDownLatch.countDown();
            })
        );

        assertTrue(countDownLatch.await(30, TimeUnit.SECONDS));
        assertNotNull(responseHolder.get());
        assertThat(clusterService.getClusterApplierService().state().getVotingConfigExclusions(), empty());
    }

    public void testTimesOutIfWaitingForNodesThatAreNotRemoved() throws InterruptedException {
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        final SetOnce<TransportException> responseHolder = new SetOnce<>();

        final ClearVotingConfigExclusionsRequest clearVotingConfigExclusionsRequest = new ClearVotingConfigExclusionsRequest();
        clearVotingConfigExclusionsRequest.setTimeout(TimeValue.timeValueMillis(100));
        transportService.sendRequest(localNode, ClearVotingConfigExclusionsAction.NAME,
            clearVotingConfigExclusionsRequest,
            expectError(e -> {
                responseHolder.set(e);
                countDownLatch.countDown();
            })
        );

        assertTrue(countDownLatch.await(30, TimeUnit.SECONDS));
        assertThat(clusterService.getClusterApplierService().state().getVotingConfigExclusions(),
                containsInAnyOrder(otherNode1Exclusion, otherNode2Exclusion));
        final Throwable rootCause = responseHolder.get().getRootCause();
        assertThat(rootCause, instanceOf(ElasticsearchTimeoutException.class));
        assertThat(rootCause.getMessage(),
            startsWith("timed out waiting for removal of nodes; if nodes should not be removed, set waitForRemoval to false. ["));
    }

    public void testSucceedsIfNodesAreRemovedWhileWaiting() throws InterruptedException {
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        final SetOnce<ClearVotingConfigExclusionsResponse> responseHolder = new SetOnce<>();

        transportService.sendRequest(localNode, ClearVotingConfigExclusionsAction.NAME,
            new ClearVotingConfigExclusionsRequest(),
            expectSuccess(r -> {
                responseHolder.set(r);
                countDownLatch.countDown();
            })
        );

        final ClusterState.Builder builder = builder(clusterService.state());
        builder.nodes(DiscoveryNodes.builder(clusterService.state().nodes()).remove(otherNode1).remove(otherNode2));
        setState(clusterService, builder);

        assertTrue(countDownLatch.await(30, TimeUnit.SECONDS));
        assertThat(clusterService.getClusterApplierService().state().getVotingConfigExclusions(), empty());
    }

    private TransportResponseHandler<ClearVotingConfigExclusionsResponse> expectSuccess(
        Consumer<ClearVotingConfigExclusionsResponse> onResponse) {
        return responseHandler(onResponse, e -> {
            throw new AssertionError("unexpected", e);
        });
    }

    private TransportResponseHandler<ClearVotingConfigExclusionsResponse> expectError(Consumer<TransportException> onException) {
        return responseHandler(r -> {
            assert false : r;
        }, onException);
    }

    private TransportResponseHandler<ClearVotingConfigExclusionsResponse> responseHandler(
        Consumer<ClearVotingConfigExclusionsResponse> onResponse, Consumer<TransportException> onException) {
        return new TransportResponseHandler<ClearVotingConfigExclusionsResponse>() {
            @Override
            public void handleResponse(ClearVotingConfigExclusionsResponse response) {
                onResponse.accept(response);
            }

            @Override
            public void handleException(TransportException exp) {
                onException.accept(exp);
            }

            @Override
            public String executor() {
                return Names.SAME;
            }

            @Override
            public ClearVotingConfigExclusionsResponse read(StreamInput in) throws IOException {
                return new ClearVotingConfigExclusionsResponse(in);
            }
        };
    }
}

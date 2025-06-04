/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.action.admin.cluster.configuration;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.coordination.CoordinationMetadata;
import org.elasticsearch.cluster.coordination.CoordinationMetadata.VotingConfigExclusion;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.node.DiscoveryNodes.Builder;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.exception.ElasticsearchTimeoutException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.transport.MockTransport;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.function.Consumer;

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
    private TransportAddVotingConfigExclusionsActionTests.FakeReconfigurator reconfigurator;

    @BeforeClass
    public static void createThreadPoolAndClusterService() {
        threadPool = new TestThreadPool("test", Settings.EMPTY);
        localNode = DiscoveryNodeUtils.create("local");
        otherNode1 = DiscoveryNodeUtils.builder("other1").name("other1").roles(emptySet()).build();
        otherNode1Exclusion = new VotingConfigExclusion(otherNode1);
        otherNode2 = DiscoveryNodeUtils.builder("other2").name("other2").roles(emptySet()).build();
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
        transportService = transport.createTransportService(
            Settings.EMPTY,
            threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            boundTransportAddress -> localNode,
            null,
            emptySet()
        );
        reconfigurator = new TransportAddVotingConfigExclusionsActionTests.FakeReconfigurator();

        new TransportClearVotingConfigExclusionsAction(
            transportService,
            clusterService,
            threadPool,
            new ActionFilters(emptySet()),
            reconfigurator
        ); // registers action

        transportService.start();
        transportService.acceptIncomingRequests();

        final ClusterState.Builder builder = builder(new ClusterName("cluster")).nodes(
            new Builder().add(localNode).add(otherNode1).add(otherNode2).localNodeId(localNode.getId()).masterNodeId(localNode.getId())
        );
        builder.metadata(
            Metadata.builder()
                .coordinationMetadata(
                    CoordinationMetadata.builder()
                        .addVotingConfigExclusion(otherNode1Exclusion)
                        .addVotingConfigExclusion(otherNode2Exclusion)
                        .build()
                )
        );
        setState(clusterService, builder);
    }

    public void testClearsVotingConfigExclusions() {
        final CountDownLatch countDownLatch = new CountDownLatch(1);

        final ClearVotingConfigExclusionsRequest clearVotingConfigExclusionsRequest = new ClearVotingConfigExclusionsRequest(
            TEST_REQUEST_TIMEOUT
        );
        clearVotingConfigExclusionsRequest.setWaitForRemoval(false);
        transportService.sendRequest(
            localNode,
            TransportClearVotingConfigExclusionsAction.TYPE.name(),
            clearVotingConfigExclusionsRequest,
            expectSuccess(r -> {
                assertNotNull(r);
                assertThat(clusterService.getClusterApplierService().state().getVotingConfigExclusions(), empty());
                countDownLatch.countDown();
            })
        );
        safeAwait(countDownLatch);
    }

    public void testTimesOutIfWaitingForNodesThatAreNotRemoved() {
        final CountDownLatch countDownLatch = new CountDownLatch(1);

        final ClearVotingConfigExclusionsRequest clearVotingConfigExclusionsRequest = new ClearVotingConfigExclusionsRequest(
            TEST_REQUEST_TIMEOUT
        );
        clearVotingConfigExclusionsRequest.setTimeout(TimeValue.timeValueMillis(100));
        transportService.sendRequest(
            localNode,
            TransportClearVotingConfigExclusionsAction.TYPE.name(),
            clearVotingConfigExclusionsRequest,
            expectError(e -> {
                assertThat(
                    clusterService.getClusterApplierService().state().getVotingConfigExclusions(),
                    containsInAnyOrder(otherNode1Exclusion, otherNode2Exclusion)
                );
                final Throwable rootCause = e.getRootCause();
                assertThat(rootCause, instanceOf(ElasticsearchTimeoutException.class));
                assertThat(
                    rootCause.getMessage(),
                    startsWith("timed out waiting for removal of nodes; if nodes should not be removed, set ?wait_for_removal=false. [")
                );
                countDownLatch.countDown();
            })
        );
        safeAwait(countDownLatch);
    }

    public void testSucceedsIfNodesAreRemovedWhileWaiting() {
        final CountDownLatch countDownLatch = new CountDownLatch(1);

        transportService.sendRequest(
            localNode,
            TransportClearVotingConfigExclusionsAction.TYPE.name(),
            new ClearVotingConfigExclusionsRequest(TEST_REQUEST_TIMEOUT),
            expectSuccess(r -> {
                assertThat(clusterService.getClusterApplierService().state().getVotingConfigExclusions(), empty());
                countDownLatch.countDown();
            })
        );

        final ClusterState.Builder builder = builder(clusterService.state());
        builder.nodes(DiscoveryNodes.builder(clusterService.state().nodes()).remove(otherNode1).remove(otherNode2));
        setState(clusterService, builder);

        safeAwait(countDownLatch);
    }

    public void testCannotClearVotingConfigurationWhenItIsDisabled() {
        final CountDownLatch countDownLatch = new CountDownLatch(1);

        reconfigurator.disableUserVotingConfigModifications();

        transportService.sendRequest(
            localNode,
            TransportClearVotingConfigExclusionsAction.TYPE.name(),
            new ClearVotingConfigExclusionsRequest(TEST_REQUEST_TIMEOUT),
            expectError(e -> {
                final Throwable rootCause = e.getRootCause();
                assertThat(rootCause, instanceOf(IllegalStateException.class));
                assertThat(rootCause.getMessage(), startsWith("Unable to modify the voting configuration"));
                countDownLatch.countDown();
            })
        );
        safeAwait(countDownLatch);
    }

    private TransportResponseHandler<ActionResponse.Empty> expectSuccess(Consumer<ActionResponse.Empty> onResponse) {
        return responseHandler(onResponse, ESTestCase::fail);
    }

    private TransportResponseHandler<ActionResponse.Empty> expectError(Consumer<TransportException> onException) {
        return responseHandler(r -> { assert false : r; }, onException);
    }

    private TransportResponseHandler<ActionResponse.Empty> responseHandler(
        Consumer<ActionResponse.Empty> onResponse,
        Consumer<TransportException> onException
    ) {
        return new TransportResponseHandler<>() {

            @Override
            public ActionResponse.Empty read(StreamInput in) {
                return ActionResponse.Empty.INSTANCE;
            }

            @Override
            public Executor executor() {
                return TransportResponseHandler.TRANSPORT_WORKER;
            }

            @Override
            public void handleResponse(ActionResponse.Empty response) {
                onResponse.accept(response);
            }

            @Override
            public void handleException(TransportException exp) {
                onException.accept(exp);
            }
        };
    }
}

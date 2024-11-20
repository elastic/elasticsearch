/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.health.node;

import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.replication.ClusterStateCreationUtils;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.health.HealthStatus;
import org.elasticsearch.health.node.UpdateHealthInfoCacheAction.Request;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;
import org.elasticsearch.test.transport.CapturingTransport;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.ClusterServiceUtils.createClusterService;
import static org.elasticsearch.test.ClusterServiceUtils.setState;
import static org.hamcrest.Matchers.equalTo;

public class UpdateHealthInfoCacheActionTests extends ESTestCase {
    private static ThreadPool threadPool;

    private ClusterService clusterService;
    private TransportService transportService;
    private DiscoveryNode localNode;
    private DiscoveryNode[] allNodes;

    @BeforeClass
    public static void beforeClass() {
        threadPool = new TestThreadPool("UpdateHealthInfoCacheAction");
    }

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        clusterService = createClusterService(threadPool);
        CapturingTransport transport = new CapturingTransport();
        transportService = transport.createTransportService(
            clusterService.getSettings(),
            threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            x -> clusterService.localNode(),
            null,
            Collections.emptySet()
        );
        transportService.start();
        transportService.acceptIncomingRequests();
        localNode = DiscoveryNodeUtils.builder("local_node")
            .roles(Set.of(DiscoveryNodeRole.MASTER_ROLE, DiscoveryNodeRole.DATA_ROLE))
            .build();
        allNodes = new DiscoveryNode[] { localNode };
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        clusterService.close();
        transportService.close();
    }

    @AfterClass
    public static void afterClass() {
        ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        threadPool = null;
    }

    public void testAction() throws ExecutionException, InterruptedException {
        DiskHealthInfo diskHealthInfo = new DiskHealthInfo(HealthStatus.GREEN, null);
        Request request = new Request.Builder().nodeId(localNode.getId()).diskHealthInfo(diskHealthInfo).build();
        PlainActionFuture<AcknowledgedResponse> listener = new PlainActionFuture<>();
        setState(clusterService, ClusterStateCreationUtils.state(localNode, localNode, localNode, allNodes));
        HealthInfoCache healthInfoCache = HealthInfoCache.create(clusterService);
        final AcknowledgedResponse expectedResponse = AcknowledgedResponse.of(true);
        ActionTestUtils.execute(
            new UpdateHealthInfoCacheAction.TransportAction(
                transportService,
                clusterService,
                threadPool,
                new ActionFilters(Set.of()),
                healthInfoCache
            ),
            null,
            request,
            listener
        );
        AcknowledgedResponse actualResponse = listener.get();
        assertThat(actualResponse, equalTo(expectedResponse));
        assertThat(healthInfoCache.getHealthInfo().diskInfoByNode().get(localNode.getId()), equalTo(diskHealthInfo));
    }

    public void testRequestSerialization() {
        // We start off with an "empty" request (i.e. only nodeId set), and let #mutateRequest change one of the fields at a time.
        Request request = new Request.Builder().nodeId(randomAlphaOfLength(10)).build();
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(
            request,
            serializedRequest -> copyWriteable(serializedRequest, writableRegistry(), Request::new),
            this::mutateRequest
        );
    }

    private Request mutateRequest(Request request) {
        String nodeId = request.getNodeId();
        DiskHealthInfo diskHealthInfo = request.getDiskHealthInfo();
        var dslHealthInfo = request.getDslHealthInfo();
        var repoHealthInfo = request.getRepositoriesHealthInfo();
        switch (randomInt(3)) {
            case 0 -> nodeId = randomAlphaOfLength(10);
            case 1 -> diskHealthInfo = randomValueOtherThan(diskHealthInfo, HealthInfoTests::randomDiskHealthInfo);
            case 2 -> dslHealthInfo = randomValueOtherThan(dslHealthInfo, HealthInfoTests::randomDslHealthInfo);
            case 3 -> repoHealthInfo = randomValueOtherThan(repoHealthInfo, HealthInfoTests::randomRepoHealthInfo);
            default -> throw new IllegalStateException();
        }
        return new Request.Builder().nodeId(nodeId)
            .diskHealthInfo(diskHealthInfo)
            .dslHealthInfo(dslHealthInfo)
            .repositoriesHealthInfo(repoHealthInfo)
            .build();
    }
}

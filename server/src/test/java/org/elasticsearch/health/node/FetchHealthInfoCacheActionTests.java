/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.health.node;

import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.replication.ClusterStateCreationUtils;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.health.HealthStatus;
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
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.ClusterServiceUtils.createClusterService;
import static org.elasticsearch.test.ClusterServiceUtils.setState;
import static org.hamcrest.Matchers.equalTo;

public class FetchHealthInfoCacheActionTests extends ESTestCase {
    private static ThreadPool threadPool;

    private ClusterService clusterService;
    private TransportService transportService;
    private DiscoveryNode localNode;
    private DiscoveryNode[] allNodes;

    @BeforeClass
    public static void beforeClass() {
        threadPool = new TestThreadPool("FetchHealthInfoCacheAction");
    }

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        CapturingTransport transport = new CapturingTransport();
        clusterService = createClusterService(threadPool);
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
        int totalNodes = randomIntBetween(1, 200);
        allNodes = new DiscoveryNode[totalNodes];
        localNode = DiscoveryNodeUtils.builder("local_node")
            .roles(Set.of(DiscoveryNodeRole.MASTER_ROLE, DiscoveryNodeRole.DATA_ROLE))
            .build();
        allNodes[0] = localNode;
        for (int i = 0; i < totalNodes - 1; i++) {
            DiscoveryNode remoteNode = DiscoveryNodeUtils.builder("remote_node" + i)
                .roles(Set.of(DiscoveryNodeRole.MASTER_ROLE, DiscoveryNodeRole.DATA_ROLE))
                .build();
            allNodes[i + 1] = remoteNode;
        }
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
        FetchHealthInfoCacheAction.Request request = new FetchHealthInfoCacheAction.Request();
        PlainActionFuture<FetchHealthInfoCacheAction.Response> listener = new PlainActionFuture<>();
        setState(clusterService, ClusterStateCreationUtils.state(localNode, localNode, localNode, allNodes));
        HealthInfoCache healthInfoCache = getTestHealthInfoCache();
        final FetchHealthInfoCacheAction.Response expectedResponse = new FetchHealthInfoCacheAction.Response(
            new HealthInfo(healthInfoCache.getHealthInfo().diskInfoByNode())
        );
        ActionTestUtils.execute(
            new FetchHealthInfoCacheAction.TransportAction(
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
        FetchHealthInfoCacheAction.Response actualResponse = listener.get();
        assertThat(actualResponse, equalTo(expectedResponse));
        assertThat(actualResponse.getHealthInfo(), equalTo(expectedResponse.getHealthInfo()));
    }

    private HealthInfoCache getTestHealthInfoCache() {
        HealthInfoCache healthInfoCache = HealthInfoCache.create(clusterService);
        for (DiscoveryNode allNode : allNodes) {
            String nodeId = allNode.getId();
            healthInfoCache.updateNodeHealth(
                nodeId,
                new DiskHealthInfo(randomFrom(HealthStatus.values()), randomFrom(DiskHealthInfo.Cause.values()))
            );
        }
        return healthInfoCache;
    }

    public void testResponseSerialization() {
        FetchHealthInfoCacheAction.Response response = new FetchHealthInfoCacheAction.Response(
            new HealthInfo(getTestHealthInfoCache().getHealthInfo().diskInfoByNode())
        );
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(
            response,
            resopnseWritable -> copyWriteable(resopnseWritable, writableRegistry(), FetchHealthInfoCacheAction.Response::new),
            this::mutateResponse
        );
    }

    private FetchHealthInfoCacheAction.Response mutateResponse(FetchHealthInfoCacheAction.Response originalResponse) {
        Map<String, DiskHealthInfo> diskHealthInfoMap = originalResponse.getHealthInfo().diskInfoByNode();
        Map<String, DiskHealthInfo> diskHealthInfoMapCopy = new HashMap<>(diskHealthInfoMap);
        diskHealthInfoMapCopy.put(
            randomAlphaOfLength(10),
            new DiskHealthInfo(randomFrom(HealthStatus.values()), randomFrom(DiskHealthInfo.Cause.values()))
        );
        return new FetchHealthInfoCacheAction.Response(new HealthInfo(diskHealthInfoMapCopy));
    }
}

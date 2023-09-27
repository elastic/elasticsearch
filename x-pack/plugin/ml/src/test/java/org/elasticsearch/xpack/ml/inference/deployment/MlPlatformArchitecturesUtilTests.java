/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.deployment;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.LatchedActionListener;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.monitor.os.OsInfo;
import org.elasticsearch.plugins.Platforms;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MlPlatformArchitecturesUtilTests extends ESTestCase {

    public void testGetNodesOsArchitectures() throws InterruptedException {
        var threadPool = mock(ThreadPool.class);
        var mockExectutorServervice = mock(ExecutorService.class);
        doNothing().when(mockExectutorServervice).execute(any());
        when(threadPool.executor(anyString())).thenReturn(mockExectutorServervice);

        var mockNodesInfoResponse = mock(NodesInfoResponse.class);
        List<NodeInfo> nodeInfoList = randomNodeInfos(4);
        when(mockNodesInfoResponse.getNodes()).thenReturn(nodeInfoList);

        var expected = nodeInfoList.stream().filter(node -> node.getNode().hasRole(DiscoveryNodeRole.ML_ROLE.roleName())).map(node -> {
            OsInfo osInfo = node.getInfo(OsInfo.class);
            return Platforms.platformName(osInfo.getName(), osInfo.getArch());
        }).collect(Collectors.toUnmodifiableSet());

        assertAsync(new Consumer<ActionListener<Set<String>>>() {
            @Override
            public void accept(ActionListener<Set<String>> setActionListener) {
                final ActionListener<NodesInfoResponse> nodesInfoResponseActionListener = MlPlatformArchitecturesUtil
                    .getArchitecturesSetFromNodesInfoResponseListener(threadPool, setActionListener);
                nodesInfoResponseActionListener.onResponse(mockNodesInfoResponse);
            }

        }, expected, null, null);
    }

    public void testVerifyMlNodesAndModelArchitectures_GivenNullModelArchitecture_ThenNothing() {
        var architectures = nArchitectures(randomIntBetween(2, 10));
        MlPlatformArchitecturesUtil.verifyMlNodesAndModelArchitectures(architectures, null, randomAlphaOfLength(10));
    }

    public void testVerifyMlNodesAndModelArchitectures_GivenZeroArches_ThenNothing() {
        var architectures = new HashSet<String>();
        MlPlatformArchitecturesUtil.verifyMlNodesAndModelArchitectures(architectures, randomAlphaOfLength(10), randomAlphaOfLength(10));
    }

    public void testVerifyMlNodesAndModelArchitectures_GivenOneArchMatches_ThenNothing() {
        Set<String> architectures = nArchitectures(1);
        String architecture = architectures.iterator().next();
        MlPlatformArchitecturesUtil.verifyMlNodesAndModelArchitectures(architectures, architecture, randomAlphaOfLength(10));
    }

    public void testVerifyMlNodesAndModelArchitectures_GivenAtLeastTwoArches_ThenThrowsISE() {
        var architectures = nArchitectures(randomIntBetween(2, 10));
        var modelId = randomAlphaOfLength(10);
        var requiredArch = randomAlphaOfLength(10);
        String message = "ML nodes in this cluster have multiple platform architectures, "
            + "but can only have one for this model (["
            + modelId
            + "]); "
            + "expected ["
            + requiredArch
            + "]; but was "
            + architectures
            + "";

        Throwable exception = expectThrows(
            IllegalStateException.class,
            "Expected IllegalStateException but no exception was thrown",
            () -> MlPlatformArchitecturesUtil.verifyMlNodesAndModelArchitectures(architectures, requiredArch, modelId)
        );
        assertEquals(exception.getMessage(), message);
    }

    public void testVerifyArchitectureMatchesModelPlatformArchitecture_GivenRequiredArchMatches_ThenNothing() {
        var requiredArch = randomAlphaOfLength(10);

        var modelId = randomAlphaOfLength(10);

        MlPlatformArchitecturesUtil.verifyMlNodesAndModelArchitectures(
            new HashSet<>(Collections.singleton(requiredArch)),
            requiredArch,
            modelId
        );
    }

    public void testVerifyArchitectureMatchesModelPlatformArchitecture_GivenRequiredArchDoesNotMatch_ThenThrowsIAE() {
        var requiredArch = randomAlphaOfLength(10);
        String architecturesStr = requiredArch + "-DIFFERENT";

        var modelId = randomAlphaOfLength(10);
        String message = "The model being deployed (["
            + modelId
            + "]) is platform specific and incompatible with ML nodes in the cluster; "
            + "expected ["
            + requiredArch
            + "]; but was ["
            + architecturesStr
            + "]";

        Throwable exception = expectThrows(
            IllegalArgumentException.class,
            "Expected IllegalArgumentException but no exception was thrown",
            () -> MlPlatformArchitecturesUtil.verifyMlNodesAndModelArchitectures(Set.of(architecturesStr), requiredArch, modelId)
        );
        assertEquals(exception.getMessage(), message);
    }

    private Set<String> nArchitectures(Integer n) {
        Set<String> architectures = new HashSet<String>(n);
        for (int i = 0; i < n; i++) {
            architectures.add(randomAlphaOfLength(10));
        }
        return architectures;
    }

    private List<NodeInfo> randomNodeInfos(int max) {
        assertTrue(max > 0);
        int n = randomInt(max);
        List<NodeInfo> nodeInfos = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            nodeInfos.add(mockNodeInfo());
        }
        return nodeInfos;
    }

    private NodeInfo mockNodeInfo() {
        var mockNodeInfo = mock(NodeInfo.class);
        var mockDiscoveryNode = mock(DiscoveryNode.class);
        when(mockNodeInfo.getNode()).thenReturn(mockDiscoveryNode);
        when(mockDiscoveryNode.hasRole(DiscoveryNodeRole.ML_ROLE.roleName())).thenReturn(randomBoolean());
        var mockOsInfo = mock(OsInfo.class);
        when(mockNodeInfo.getInfo(OsInfo.class)).thenReturn(mockOsInfo);
        when(mockOsInfo.getArch()).thenReturn(randomAlphaOfLength(10));
        when(mockOsInfo.getName()).thenReturn(randomAlphaOfLength(10));

        return mockNodeInfo;
    }

    protected <T> void assertAsync(
        Consumer<ActionListener<T>> function,
        T expected,
        CheckedConsumer<T, ? extends Exception> onAnswer,
        Consumer<Exception> onException
    ) throws InterruptedException {

        CountDownLatch latch = new CountDownLatch(1);

        LatchedActionListener<T> listener = new LatchedActionListener<>(ActionListener.wrap(r -> {
            if (expected == null) {
                fail("expected an exception but got a response");
            } else {
                assertThat(r, equalTo(expected));
            }
            if (onAnswer != null) {
                onAnswer.accept(r);
            }
        }, e -> {
            if (onException == null) {
                logger.error("got unexpected exception", e);
                fail("got unexpected exception: " + e.getMessage());
            } else {
                onException.accept(e);
            }
        }), latch);

        function.accept(listener);
        latch.countDown();
        assertTrue("timed out after 20s", latch.await(20, TimeUnit.SECONDS));
    }
}

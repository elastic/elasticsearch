/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation.allocator;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class DesiredBalanceShardsAllocatorTests extends ESTestCase {

    private static ThreadPool threadPool;

    @BeforeClass
    public static void createThreadPool() {
        assertNull(threadPool);
        threadPool = new TestThreadPool("test");
    }

    @AfterClass
    public static void terminateThreadPool() {
        try {
            assertTrue(ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS));
        } finally {
            threadPool = null;
        }
    }

    private static DiscoveryNodes randomDiscoveryNodes() {
        final DiscoveryNodes.Builder builder = DiscoveryNodes.builder();

        final DiscoveryNode localNode = new DiscoveryNode(
            "master-node",
            buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            Set.of(DiscoveryNodeRole.MASTER_ROLE),
            Version.CURRENT
        );

        builder.add(localNode);
        builder.localNodeId(localNode.getId());
        builder.masterNodeId(localNode.getId());

        for (int i = between(1, 5); i > 0; i--) {
            builder.add(randomDataNode("data-node-" + i));
        }
        for (int i = between(1, 5); i > 0; i--) {
            builder.add(randomNonDataNode("non-data-node-" + i));
        }

        return builder.build();
    }

    private static DiscoveryNode randomDataNode(String nodeId) {
        return new DiscoveryNode(
            nodeId,
            buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            new HashSet<>(
                randomValueOtherThanMany(
                    roles -> roles.stream().noneMatch(DiscoveryNodeRole::canContainData),
                    () -> randomSubsetOf(DiscoveryNodeRole.roles())
                )
            ),
            Version.CURRENT
        );
    }

    private static DiscoveryNode randomNonDataNode(String nodeId) {
        return new DiscoveryNode(
            nodeId,
            buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            new HashSet<>(
                randomSubsetOf(DiscoveryNodeRole.roles().stream().filter(r -> r.canContainData() == false).collect(Collectors.toSet()))
            ),
            Version.CURRENT
        );
    }

}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.NoSuchRemoteClusterException;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class ClusterNameExpressionResolverTests extends ESTestCase {

    private static final Set<String> remoteClusters = new HashSet<>();

    static {
        remoteClusters.add("cluster1");
        remoteClusters.add("cluster2");
        remoteClusters.add("totallyDifferent");
    }

    public void testExactMatch() {
        List<String> clusters = ClusterNameExpressionResolver.resolveClusterNames(remoteClusters, "totallyDifferent");
        assertEquals(new HashSet<>(Arrays.asList("totallyDifferent")), new HashSet<>(clusters));
    }

    public void testNoWildCardNoMatch() {
        expectThrows(
            NoSuchRemoteClusterException.class,
            () -> ClusterNameExpressionResolver.resolveClusterNames(remoteClusters, "totallyDifferent2")
        );
    }

    public void testWildCardNoMatch() {
        List<String> clusters = ClusterNameExpressionResolver.resolveClusterNames(remoteClusters, "totally*2");
        assertTrue(clusters.isEmpty());
    }

    public void testSimpleWildCard() {
        List<String> clusters = ClusterNameExpressionResolver.resolveClusterNames(remoteClusters, "*");
        assertEquals(new HashSet<>(Arrays.asList("cluster1", "cluster2", "totallyDifferent")), new HashSet<>(clusters));
    }

    public void testSuffixWildCard() {
        List<String> clusters = ClusterNameExpressionResolver.resolveClusterNames(remoteClusters, "cluster*");
        assertEquals(new HashSet<>(Arrays.asList("cluster1", "cluster2")), new HashSet<>(clusters));
    }

    public void testPrefixWildCard() {
        List<String> clusters = ClusterNameExpressionResolver.resolveClusterNames(remoteClusters, "*Different");
        assertEquals(new HashSet<>(Arrays.asList("totallyDifferent")), new HashSet<>(clusters));
    }

    public void testMiddleWildCard() {
        List<String> clusters = ClusterNameExpressionResolver.resolveClusterNames(remoteClusters, "clu*1");
        assertEquals(new HashSet<>(Arrays.asList("cluster1")), new HashSet<>(clusters));
    }
}

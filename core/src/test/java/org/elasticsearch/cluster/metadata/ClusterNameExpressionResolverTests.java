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

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class ClusterNameExpressionResolverTests extends ESTestCase {

    private ClusterNameExpressionResolver clusterNameResolver = new ClusterNameExpressionResolver(Settings.EMPTY);
    private static final Set<String> remoteClusters = new HashSet<>();

    static {
        remoteClusters.add("cluster1");
        remoteClusters.add("cluster2");
        remoteClusters.add("totallyDifferent");
    }

    public void testExactMatch() {
        List<String> clusters = clusterNameResolver.resolveClusterNames(remoteClusters, "totallyDifferent");
        assertEquals(new HashSet<>(Arrays.asList("totallyDifferent")), new HashSet<>(clusters));
    }

    public void testNoWildCardNoMatch() {
        List<String> clusters = clusterNameResolver.resolveClusterNames(remoteClusters, "totallyDifferent2");
        assertTrue(clusters.isEmpty());
    }

    public void testWildCardNoMatch() {
        List<String> clusters = clusterNameResolver.resolveClusterNames(remoteClusters, "totally*2");
        assertTrue(clusters.isEmpty());
    }

    public void testSimpleWildCard() {
        List<String> clusters = clusterNameResolver.resolveClusterNames(remoteClusters, "*");
        assertEquals(new HashSet<>(Arrays.asList("cluster1", "cluster2", "totallyDifferent")), new HashSet<>(clusters));
    }

    public void testSuffixWildCard() {
        List<String> clusters = clusterNameResolver.resolveClusterNames(remoteClusters, "cluster*");
        assertEquals(new HashSet<>(Arrays.asList("cluster1", "cluster2")), new HashSet<>(clusters));
    }

    public void testPrefixWildCard() {
        List<String> clusters = clusterNameResolver.resolveClusterNames(remoteClusters, "*Different");
        assertEquals(new HashSet<>(Arrays.asList("totallyDifferent")), new HashSet<>(clusters));
    }

    public void testMiddleWildCard() {
        List<String> clusters = clusterNameResolver.resolveClusterNames(remoteClusters, "clu*1");
        assertEquals(new HashSet<>(Arrays.asList("cluster1")), new HashSet<>(clusters));
    }
}

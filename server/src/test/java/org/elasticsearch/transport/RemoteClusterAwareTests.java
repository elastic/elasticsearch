/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.transport;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;

public class RemoteClusterAwareTests extends ESTestCase {

    public void testBuildRemoteIndexName() {
        {
            String clusterAlias = randomAlphaOfLengthBetween(5, 10);
            String index = randomAlphaOfLengthBetween(5, 10);
            String remoteIndexName = RemoteClusterAware.buildRemoteIndexName(clusterAlias, index);
            assertEquals(clusterAlias + ":" + index, remoteIndexName);
        }
        {
            String index = randomAlphaOfLengthBetween(5, 10);
            String clusterAlias = randomBoolean() ? null : RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY;
            String remoteIndexName = RemoteClusterAware.buildRemoteIndexName(clusterAlias, index);
            assertEquals(index, remoteIndexName);
        }
    }

    public void testGroupClusterIndices() {
        RemoteClusterAwareTest remoteClusterAware = new RemoteClusterAwareTest();
        Set<String> remoteClusterNames = Set.of("cluster1", "cluster2", "some-cluster3");
        String[] requestIndices = new String[] {
            "index1",
            "index2",
            "*",
            "-index3",
            "cluster1:index2",
            "cluster2:*",
            "cluster*:index1",
            "<datemath-{2001-01-01-13||+1h/h{yyyy-MM-dd-HH|-07:00}}>",
            "cluster1:<datemath-{2001-01-01-13||+1h/h{yyyy-MM-dd-HH|-07:00}}>",
            "-<datemath-{2001-01-01-13||+1h/h{yyyy-MM-dd-HH|00:00}}>",
            "cluster2:-<datemath-{2001-01-01-13||+1h/h{yyyy-MM-dd-HH|-07:00}}>", };

        Map<String, List<String>> groupedIndices = remoteClusterAware.groupClusterIndices(remoteClusterNames, requestIndices);
        assertEquals(3, groupedIndices.size());
        assertThat(groupedIndices, hasKey(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY));
        assertThat(groupedIndices, hasKey("cluster1"));
        assertThat(groupedIndices, hasKey("cluster2"));
        assertThat(groupedIndices, not(hasKey("some-cluster3")));

        assertThat(groupedIndices.get(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY).size(), equalTo(6));
        assertThat(
            groupedIndices.get(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY),
            containsInAnyOrder(
                "index1",
                "index2",
                "*",
                "-index3",
                "<datemath-{2001-01-01-13||+1h/h{yyyy-MM-dd-HH|-07:00}}>",
                "-<datemath-{2001-01-01-13||+1h/h{yyyy-MM-dd-HH|00:00}}>"
            )
        );

        assertThat(groupedIndices.get("cluster1").size(), equalTo(3));
        assertThat(
            groupedIndices.get("cluster1"),
            containsInAnyOrder("index2", "index1", "<datemath-{2001-01-01-13||+1h/h{yyyy-MM-dd-HH|-07:00}}>")
        );

        assertThat(groupedIndices.get("cluster2").size(), equalTo(3));
        assertThat(
            groupedIndices.get("cluster2"),
            containsInAnyOrder("*", "index1", "-<datemath-{2001-01-01-13||+1h/h{yyyy-MM-dd-HH|-07:00}}>")
        );
    }

    public void testGroupClusterIndicesWildcard() {
        RemoteClusterAwareTest remoteClusterAware = new RemoteClusterAwareTest();
        Set<String> remoteClusterNames = Set.of("cluster1", "cluster2", "some-cluster3");
        String[] requestIndices = new String[] {
            "*",
            "*:*",
            "cluster2*:index*",
            "cluster*:index1",
            "-some-*:*",
            "-index*",
            "cluster*:-noindex" };

        Map<String, List<String>> groupedIndices = remoteClusterAware.groupClusterIndices(remoteClusterNames, requestIndices);
        assertEquals(3, groupedIndices.size());
        assertThat(groupedIndices, hasKey(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY));
        assertThat(groupedIndices, hasKey("cluster1"));
        assertThat(groupedIndices, hasKey("cluster2"));
        assertThat(groupedIndices, not(hasKey("some-cluster3")));

        assertThat(groupedIndices.get(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY).size(), equalTo(2));
        assertThat(groupedIndices.get(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY), containsInAnyOrder("*", "-index*"));

        assertThat(groupedIndices.get("cluster1").size(), equalTo(3));
        assertThat(groupedIndices.get("cluster1"), containsInAnyOrder("*", "index1", "-noindex"));

        assertThat(groupedIndices.get("cluster2").size(), equalTo(4));
        assertThat(groupedIndices.get("cluster2"), containsInAnyOrder("*", "index*", "index1", "-noindex"));
    }

    private static <T extends Throwable> void mustThrowException(String[] requestIndices, Class<T> expectedType, String expectedMessage) {
        RemoteClusterAwareTest remoteClusterAware = new RemoteClusterAwareTest();
        Set<String> remoteClusterNames = Set.of("cluster1", "cluster2", "some-cluster3");
        assertThat(
            expectThrows(expectedType, () -> remoteClusterAware.groupClusterIndices(remoteClusterNames, requestIndices)).getMessage(),
            containsString(expectedMessage)
        );
    }

    public void testGroupClusterIndicesFail() {
        RemoteClusterAwareTest remoteClusterAware = new RemoteClusterAwareTest();
        Set<String> remoteClusterNames = Set.of("cluster1", "cluster2", "some-cluster3");

        mustThrowException(new String[] { ":foo" }, IllegalArgumentException.class, "is invalid because the remote part is empty");
        mustThrowException(new String[] { "notacluster:foo" }, NoSuchRemoteClusterException.class, "no such remote cluster");
        // Cluster wildcard exclusion requires :*
        mustThrowException(
            new String[] { "*:*", "-cluster*:index1" },
            IllegalArgumentException.class,
            "To exclude a cluster you must specify the '*' wildcard"
        );
        mustThrowException(
            new String[] { "*:*", "-cluster2:index1" },
            IllegalArgumentException.class,
            "To exclude a cluster you must specify the '*' wildcard"
        );
        // Excluding a cluster that we didn't previously include
        mustThrowException(
            new String[] { "cluster1:*", "-some*:*" },
            IllegalArgumentException.class,
            "not included in the list of clusters to be included"
        );
        // Excluded all clusters
        mustThrowException(
            new String[] { "*:index1", "-some*:*", "-cluster*:*" },
            IllegalArgumentException.class,
            "The '-' exclusions in the index expression list excludes all indexes"
        );

    }

    private static class RemoteClusterAwareTest extends RemoteClusterAware {
        RemoteClusterAwareTest() {
            super(Settings.EMPTY);
        }

        @Override
        protected void updateRemoteCluster(String clusterAlias, Settings settings) {

        }

        @Override
        public Map<String, List<String>> groupClusterIndices(Set<String> remoteClusterNames, String[] requestIndices) {
            return super.groupClusterIndices(remoteClusterNames, requestIndices);
        }
    }
}

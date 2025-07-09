/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.RemoteClusterService;

import java.util.List;

public class EsqlExecutionInfoTests extends ESTestCase {

    static final EsqlExecutionInfo.Cluster localCluster = new EsqlExecutionInfo.Cluster(
        RemoteClusterService.LOCAL_CLUSTER_GROUP_KEY,
        "test"
    );
    static final EsqlExecutionInfo.Cluster remoteCluster = new EsqlExecutionInfo.Cluster("remote", "test");

    public void testHasMetadataInclude() {
        // includeCCSMetadata + non-local clusters will produce true
        EsqlExecutionInfo info = new EsqlExecutionInfo(true);
        assertFalse(info.hasMetadataToReport());
        info.swapCluster(RemoteClusterService.LOCAL_CLUSTER_GROUP_KEY, (k, v) -> localCluster);
        assertFalse(info.hasMetadataToReport());
        info.swapCluster("remote", (k, v) -> remoteCluster);
        assertTrue(info.hasMetadataToReport());
        // Only remote is enough
        info = new EsqlExecutionInfo(true);
        info.swapCluster("remote", (k, v) -> remoteCluster);
        assertTrue(info.hasMetadataToReport());
    }

    public void testHasMetadataIncludeFalse() {
        // If includeCCSMetadata is false, then it should always return false
        EsqlExecutionInfo info = new EsqlExecutionInfo(false);
        assertFalse(info.hasMetadataToReport());
        assertFalse(info.hasMetadataToReport());
        info.swapCluster(RemoteClusterService.LOCAL_CLUSTER_GROUP_KEY, (k, v) -> localCluster);
        assertFalse(info.hasMetadataToReport());
        info.swapCluster("remote", (k, v) -> remoteCluster);
        assertFalse(info.hasMetadataToReport());
    }

    public void testHasMetadataPartial() {
        EsqlExecutionInfo info = new EsqlExecutionInfo(false);
        String key = randomFrom(RemoteClusterService.LOCAL_CLUSTER_GROUP_KEY, "remote");
        info.swapCluster(key, (k, v) -> new EsqlExecutionInfo.Cluster(k, "test", false, EsqlExecutionInfo.Cluster.Status.SUCCESSFUL));
        assertFalse(info.isPartial());
        assertFalse(info.hasMetadataToReport());
        info.swapCluster(key, (k, v) -> new EsqlExecutionInfo.Cluster(k, "test", false, EsqlExecutionInfo.Cluster.Status.PARTIAL));
        assertTrue(info.isPartial());
        assertFalse(info.hasMetadataToReport());
        info.swapCluster(key, (k, v) -> {
            EsqlExecutionInfo.Cluster.Builder builder = new EsqlExecutionInfo.Cluster.Builder(v);
            builder.setFailures(List.of(new ShardSearchFailure(new IllegalStateException("shard failure"))));
            return builder.build();
        });
        assertTrue(info.hasMetadataToReport());
    }

}

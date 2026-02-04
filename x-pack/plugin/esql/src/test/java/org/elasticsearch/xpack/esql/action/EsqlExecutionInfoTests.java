/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.core.Predicates;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.RemoteClusterService;

import java.util.List;

public class EsqlExecutionInfoTests extends ESTestCase {

    static final EsqlExecutionInfo.Cluster localCluster = createEsqlExecutionInfoCluster(
        RemoteClusterService.LOCAL_CLUSTER_GROUP_KEY,
        "test"
    );
    static final EsqlExecutionInfo.Cluster remoteCluster = createEsqlExecutionInfoCluster("remote", "test");

    public void testHasMetadataInclude() {
        // includeCCSMetadata + non-local clusters will produce true
        EsqlExecutionInfo info = createEsqlExecutionInfo(true);
        assertFalse(info.hasMetadataToReport());
        info.swapCluster(RemoteClusterService.LOCAL_CLUSTER_GROUP_KEY, (k, v) -> localCluster);
        assertFalse(info.hasMetadataToReport());
        info.swapCluster("remote", (k, v) -> remoteCluster);
        assertTrue(info.hasMetadataToReport());
        // Only remote is enough
        info = createEsqlExecutionInfo(true);
        info.swapCluster("remote", (k, v) -> remoteCluster);
        assertTrue(info.hasMetadataToReport());
    }

    public void testHasMetadataIncludeFalse() {
        // If includeCCSMetadata is false, then it should always return false
        EsqlExecutionInfo info = createEsqlExecutionInfo(false);
        assertFalse(info.hasMetadataToReport());
        assertFalse(info.hasMetadataToReport());
        info.swapCluster(RemoteClusterService.LOCAL_CLUSTER_GROUP_KEY, (k, v) -> localCluster);
        assertFalse(info.hasMetadataToReport());
        info.swapCluster("remote", (k, v) -> remoteCluster);
        assertFalse(info.hasMetadataToReport());
    }

    public void testHasMetadataPartial() {
        EsqlExecutionInfo info = createEsqlExecutionInfo(false);
        String key = randomFrom(RemoteClusterService.LOCAL_CLUSTER_GROUP_KEY, "remote");
        info.swapCluster(key, (k, v) -> createEsqlExecutionInfoCluster(k, "test", false, EsqlExecutionInfo.Cluster.Status.SUCCESSFUL));
        assertFalse(info.isPartial());
        assertFalse(info.hasMetadataToReport());
        info.swapCluster(key, (k, v) -> createEsqlExecutionInfoCluster(k, "test", false, EsqlExecutionInfo.Cluster.Status.PARTIAL));
        assertTrue(info.isPartial());
        assertFalse(info.hasMetadataToReport());
        info.swapCluster(key, (k, v) -> {
            EsqlExecutionInfo.Cluster.Builder builder = new EsqlExecutionInfo.Cluster.Builder(v);
            builder.addFailures(List.of(new ShardSearchFailure(new IllegalStateException("shard failure"))));
            return builder.build();
        });
        assertTrue(info.hasMetadataToReport());
    }

    public static EsqlExecutionInfo createEsqlExecutionInfo(boolean includeCCSMetadata) {
        return new EsqlExecutionInfo(
            Predicates.always(),
            includeCCSMetadata ? EsqlExecutionInfo.IncludeExecutionMetadata.CCS_ONLY : EsqlExecutionInfo.IncludeExecutionMetadata.NEVER
        );
    }

    public static EsqlExecutionInfo.Cluster createEsqlExecutionInfoCluster(String clusterAlias, String indexExpression) {
        return new EsqlExecutionInfo.Cluster(
            clusterAlias,
            clusterAlias,
            indexExpression,
            true,
            EsqlExecutionInfo.Cluster.Status.RUNNING,
            null,
            null,
            null,
            null,
            null,
            null
        );
    }

    public static EsqlExecutionInfo.Cluster createEsqlExecutionInfoCluster(
        String clusterAlias,
        String indexExpression,
        boolean skipUnavailable,
        EsqlExecutionInfo.Cluster.Status status
    ) {
        return new EsqlExecutionInfo.Cluster(
            clusterAlias,
            clusterAlias,
            indexExpression,
            skipUnavailable,
            status,
            null,
            null,
            null,
            null,
            null,
            null
        );
    }
}

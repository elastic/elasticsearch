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
import org.elasticsearch.xpack.core.esql.action.EsqlClusterInfo;

import java.time.ZoneOffset;
import java.util.List;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;

public class EsqlResponseImplTests extends ESTestCase {

    private static List<EsqlClusterInfo> clusters(EsqlExecutionInfo info) {
        try (
            EsqlQueryResponse response = new EsqlQueryResponse(
                List.of(),
                List.of(),
                0L,
                0L,
                null,
                false,
                null,
                false,
                false,
                ZoneOffset.UTC,
                0,
                0,
                info
            )
        ) {
            return response.response().clusters();
        }
    }

    public void testClustersInAliasOrder() {
        EsqlExecutionInfo info = new EsqlExecutionInfo(Predicates.always(), EsqlExecutionInfo.IncludeExecutionMetadata.ALWAYS);
        // insert out of order, the view must sort by alias
        info.swapCluster(
            "remote1",
            (k, v) -> EsqlExecutionInfoTests.createEsqlExecutionInfoCluster(k, "logs-*", EsqlExecutionInfo.Cluster.Status.SKIPPED)
        );
        info.swapCluster(
            RemoteClusterService.LOCAL_CLUSTER_GROUP_KEY,
            (k, v) -> EsqlExecutionInfoTests.createEsqlExecutionInfoCluster(k, "logs-*", EsqlExecutionInfo.Cluster.Status.SUCCESSFUL)
        );

        assertThat(
            clusters(info),
            contains(
                new EsqlClusterInfo(EsqlClusterInfo.LOCAL_CLUSTER_ALIAS, "logs-*", EsqlClusterInfo.Status.SUCCESSFUL),
                new EsqlClusterInfo("remote1", "logs-*", EsqlClusterInfo.Status.SKIPPED)
            )
        );
    }

    public void testNoClustersWhenNothingToReport() {
        EsqlExecutionInfo info = new EsqlExecutionInfo(Predicates.always(), EsqlExecutionInfo.IncludeExecutionMetadata.NEVER);
        info.swapCluster(
            RemoteClusterService.LOCAL_CLUSTER_GROUP_KEY,
            (k, v) -> EsqlExecutionInfoTests.createEsqlExecutionInfoCluster(k, "logs-*", EsqlExecutionInfo.Cluster.Status.SUCCESSFUL)
        );
        assertThat(clusters(info), empty());
    }

    public void testNoClustersWithoutExecutionInfo() {
        assertThat(clusters(null), empty());
    }

    public void testPartialFailureReportsClustersWithoutMetadataRequested() {
        EsqlExecutionInfo info = new EsqlExecutionInfo(Predicates.always(), EsqlExecutionInfo.IncludeExecutionMetadata.NEVER);
        // A successful cluster without failures is not reported, as in the REST response.
        info.swapCluster(
            EsqlClusterInfo.LOCAL_CLUSTER_ALIAS,
            (k, v) -> EsqlExecutionInfoTests.createEsqlExecutionInfoCluster(k, "logs-*", EsqlExecutionInfo.Cluster.Status.SUCCESSFUL)
        );
        // swapCluster marks the info partial because the new cluster is PARTIAL
        info.swapCluster(
            "remote1",
            (k, v) -> new EsqlExecutionInfo.Cluster.Builder(
                EsqlExecutionInfoTests.createEsqlExecutionInfoCluster(k, "logs-*", EsqlExecutionInfo.Cluster.Status.PARTIAL)
            ).addFailures(List.of(new ShardSearchFailure(new IllegalStateException("shard failure")))).build()
        );
        assertTrue(info.isPartial());

        assertThat(clusters(info), contains(new EsqlClusterInfo("remote1", "logs-*", EsqlClusterInfo.Status.PARTIAL)));
    }

    public void testEveryClusterStatusIsMapped() {
        // valueOf throws for any EsqlExecutionInfo.Cluster.Status constant without a core EsqlClusterInfo.Status mirror,
        // so this fails when a status is added to the server enum but not to the core one.
        for (EsqlExecutionInfo.Cluster.Status status : EsqlExecutionInfo.Cluster.Status.values()) {
            assertEquals(status.name(), EsqlClusterInfo.Status.valueOf(status.name()).name());
        }
    }
}

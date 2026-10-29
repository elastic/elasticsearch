/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.esql.datasource.csv.CsvDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.equalTo;

/** Exercises broadcast inputs on split-assigned stages, which bypass the shard-driver execution path. */
@ESIntegTestCase.ClusterScope(numDataNodes = 2, numClientNodes = 1, supportsDedicatedMasters = false)
public class ShuffleExternalIT extends AbstractExternalDataSourceIT {
    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(CsvDataSourcePlugin.class);
    }

    public void testBroadcastJoinOnExternalProbe() throws Exception {
        assumeTrue("JOIN on a subquery required", EsqlCapabilities.Cap.JOIN_ON_SUBQUERY.isEnabled());
        Path probeDir = createTempDir();
        for (int i = 0; i < 4; i++) {
            Files.writeString(
                probeDir.resolve(i + ".csv"),
                "user_id:long,bytes:long\n" + i % 3 + "," + (i + 1) + "\n" + (i + 1) % 3 + "," + (i + 5) + "\n"
            );
        }
        Path buildFile = createTempFile("broadcast-build", ".csv");
        Files.writeString(buildFile, "user_id:long,segment:keyword\n0,a\n0,b\n1,c\n");
        registerDataset("shuffle_probe", StoragePath.fileUri(probeDir) + "/*.csv", Map.of());
        registerDataset("shuffle_build", StoragePath.fileUri(buildFile), Map.of());
        String coordinator = clusterService().state()
            .nodes()
            .stream()
            .filter(node -> node.canContainData() == false)
            .map(DiscoveryNode::getName)
            .findFirst()
            .orElseThrow();
        QueryPragmas pragmas = new QueryPragmas(
            Settings.builder()
                .put(QueryPragmas.SHUFFLE_PARTITIONS_PER_NODE.getKey(), 2)
                .put(QueryPragmas.SHUFFLE_JOIN.getKey(), true)
                .put(QueryPragmas.SHUFFLE_JOIN_STRATEGY.getKey(), "broadcast")
                .put(QueryPragmas.EXCHANGE_BUFFER_SIZE.getKey(), 1)
                .put(QueryPragmas.TASK_CONCURRENCY.getKey(), 2)
                .build()
        );
        for (String joinType : List.of("LEFT OUTER", "INNER")) {
            String query = "FROM shuffle_probe | "
                + joinType
                + " JOIN (FROM shuffle_build) ON user_id | KEEP user_id, bytes, segment | SORT user_id, bytes, segment";
            try (
                EsqlQueryResponse baseline = client(coordinator).execute(EsqlQueryAction.INSTANCE, syncEsqlQueryRequest(query))
                    .actionGet(TIMEOUT);
                EsqlQueryResponse broadcast = client(coordinator).execute(
                    EsqlQueryAction.INSTANCE,
                    syncEsqlQueryRequest(query).acceptedPragmaRisks(true).pragmas(pragmas).profile(true)
                ).actionGet(TIMEOUT)
            ) {
                assertThat(broadcast.columns(), equalTo(baseline.columns()));
                assertThat(getValuesList(broadcast), equalTo(getValuesList(baseline)));
                assertTrue(
                    "broadcast join must run on a split-assigned data node",
                    broadcast.profile()
                        .plans()
                        .stream()
                        .anyMatch(
                            plan -> plan.nodeName().equals(coordinator) == false
                                && plan.description().startsWith("stage")
                                && plan.planTree().contains("DistributedHashJoinExec")
                        )
                );
                assertTrue(
                    "build rows must flow through a broadcast edge",
                    broadcast.profile()
                        .plans()
                        .stream()
                        .anyMatch(plan -> plan.planTree().contains("StageSinkExec") && plan.planTree().contains("BROADCAST"))
                );
                assertFalse(
                    "the coordinator must not execute the broadcast join",
                    broadcast.profile()
                        .plans()
                        .stream()
                        .anyMatch(plan -> plan.nodeName().equals(coordinator) && plan.planTree().contains("DistributedHashJoinExec"))
                );
            }
        }
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.operator.exchange.ExchangeService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.xpack.esql.datasource.csv.CsvDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.plugin.ComputeService;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;

/**
 * Tests that an exchange sink that no data node request will claim is removed from the external worker at once, not by its
 * inactive-sink reaper.
 * <p>
 * The query is started on a coordinating-only node, which is not an external worker, so every split goes to the one data node.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = false)
public class ExternalUnclaimedExchangeSinkIT extends AbstractExternalDataSourceIT {

    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(CsvDataSourcePlugin.class);
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(MockTransportService.TestPlugin.class);
        plugins.add(InternalExchangePlugin.class);
        return plugins;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(ExchangeService.INACTIVE_SINKS_INTERVAL_SETTING, EsqlUnclaimedExchangeSinkIT.INACTIVE_SINKS_INTERVAL)
            .build();
    }

    /**
     * A query cancelled between the first open on a worker and the creation of the compute group task of that worker leaves the
     * worker with a sink that no data node request will claim. The coordinator removes it.
     */
    public void testCancelBeforeGroupTaskRemovesOpenedSink() throws Exception {
        String worker = internalCluster().getMasterName();
        String coordinator = internalCluster().startCoordinatingOnlyNode(Settings.EMPTY);

        Path root = createTempDir().resolve("unclaimed");
        Files.createDirectories(root);
        Files.writeString(root.resolve("part.csv"), "id\n1\n", StandardCharsets.UTF_8);
        String dataset = registerDataset("unclaimed", StoragePath.fileUri(root) + "/*.csv", Map.of());

        // the sum forces a scan on the worker: a count alone can be answered from the metadata of the files
        EsqlQueryRequest request = syncEsqlQueryRequest("FROM " + dataset + " | STATS COUNT(*), SUM(id)");
        request.pragmas(new QueryPragmas(Settings.builder().put(QueryPragmas.EXTERNAL_DISTRIBUTION.getKey(), "round_robin").build()));
        request.acceptedPragmaRisks(true);
        EsqlUnclaimedExchangeSinkIT.assertCancelledQueryRemovesSink(
            EsqlUnclaimedExchangeSinkIT.CancelPoint.BEFORE_GROUP_TASK,
            client(coordinator),
            MockTransportService.getInstance(coordinator),
            Map.of(MockTransportService.getInstance(worker), internalCluster().getInstance(ExchangeService.class, worker)),
            ComputeService.DATA_ACTION_NAME,
            request
        );
    }
}

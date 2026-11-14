/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.TransportService;
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
import static org.elasticsearch.xpack.esql.action.SerializedPlanBreakerIT.NUM_DOCS;
import static org.elasticsearch.xpack.esql.action.SerializedPlanBreakerIT.QUERY_TAIL;
import static org.elasticsearch.xpack.esql.action.SerializedPlanBreakerIT.SERIALIZED_REQUEST_LABEL;
import static org.elasticsearch.xpack.esql.action.SerializedPlanBreakerIT.assertTrippedOn;
import static org.elasticsearch.xpack.esql.action.SerializedPlanBreakerIT.fillBreakerAfterNthSend;
import static org.elasticsearch.xpack.esql.action.SerializedPlanBreakerIT.key;

/**
 * The coordinator charges its request breaker for each serialized request that carries the sub-plan result to an external
 * worker, until that request has been written. A dedicated coordinator is never a worker, and the breaker is filled once the
 * first worker's main-plan request has been written, so only the second one trips.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, numClientNodes = 0)
public class ExternalSerializedPlanBreakerIT extends AbstractExternalDataSourceIT {

    private static final int FILES = 15;

    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(CsvDataSourcePlugin.class);
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(MockTransportService.TestPlugin.class);
        return plugins;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(SerializedPlanBreakerIT.memoryRequestBreaker())
            .build();
    }

    public void testTripFailsQueryWithoutPartialResults() throws Exception {
        assertTripFailsQuery(false);
    }

    /** A failed external worker fails the whole query, so a trip does too, even when partial results are allowed. */
    public void testTripFailsQueryWithPartialResults() throws Exception {
        assertTripFailsQuery(true);
    }

    private void assertTripFailsQuery(boolean allowPartialResults) throws Exception {
        internalCluster().startMasterOnlyNode();
        internalCluster().startDataOnlyNodes(2);
        String coordinator = internalCluster().startCoordinatingOnlyNode(Settings.EMPTY);
        ensureStableCluster(4);
        registerDataSource("breaker_ds", Map.of());
        registerDataset("breaker_dataset", "breaker_ds", StoragePath.fileUri(writeFixture()) + "/*.csv", Map.of());

        var request = syncEsqlQueryRequest("FROM breaker_dataset" + QUERY_TAIL).allowPartialResults(allowPartialResults);
        request.pragmas(new QueryPragmas(Settings.builder().put(QueryPragmas.EXTERNAL_DISTRIBUTION.getKey(), "round_robin").build()));
        request.acceptedPragmaRisks(true);
        try (
            Releasable filled = fillBreakerAfterNthSend(
                (MockTransportService) internalCluster().getInstance(TransportService.class, coordinator),
                internalCluster().getInstance(CircuitBreakerService.class, coordinator).getBreaker(CircuitBreaker.REQUEST),
                ComputeService.DATA_ACTION_NAME,
                3
            )
        ) {
            Exception e = expectThrows(
                Exception.class,
                () -> client(coordinator).execute(EsqlQueryAction.INSTANCE, request).actionGet(TIMEOUT).close()
            );
            assertTrippedOn(e, SERIALIZED_REQUEST_LABEL);
        }
    }

    private Path writeFixture() throws Exception {
        Path root = createTempDir().resolve("breaker_dataset");
        Files.createDirectories(root);
        int rowsPerFile = NUM_DOCS / FILES;
        for (int f = 0; f < FILES; f++) {
            StringBuilder body = new StringBuilder("k\n");
            for (int i = 0; i < rowsPerFile; i++) {
                body.append(key(f * rowsPerFile + i)).append('\n');
            }
            Files.writeString(root.resolve("part_" + f + ".csv"), body.toString(), StandardCharsets.UTF_8);
        }
        return root;
    }
}

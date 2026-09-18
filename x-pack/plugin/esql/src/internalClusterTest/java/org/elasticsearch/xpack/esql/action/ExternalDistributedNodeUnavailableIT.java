/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.NodeNotConnectedException;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.datasource.csv.CsvDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;

public class ExternalDistributedNodeUnavailableIT extends AbstractExternalDataSourceIT {

    private static final int FILES = 8;
    private static final int ROWS_PER_FILE = 100;
    private static final long ROWS = (long) FILES * ROWS_PER_FILE;
    /** Sum of ids 0 .. ROWS - 1; forces a real scan rather than an answer folded from file metadata. */
    private static final long ID_SUM = ROWS * (ROWS - 1) / 2;

    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(CsvDataSourcePlugin.class);
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), MockTransportService.TestPlugin.class);
    }

    public void testUnreachableNodeAtDispatchIsNotASilentPartialAnswer() throws Exception {
        runUnreachableNodeAtDispatch(true);
    }

    public void testUnreachableNodeAtDispatchIsReassignedWhenPartialResultsDisallowed() throws Exception {
        runUnreachableNodeAtDispatch(false);
    }

    private void runUnreachableNodeAtDispatch(boolean allowPartial) throws Exception {
        internalCluster().ensureAtLeastNumDataNodes(2);
        List<String> dataNodes = Arrays.stream(internalCluster().getNodeNames()).filter(n -> isDataNode(n)).toList();
        String coordinator = dataNodes.get(0);
        String unreachable = dataNodes.get(1);

        Path root = createTempDir().resolve("unreachable_node");
        Files.createDirectories(root);
        for (int f = 0; f < FILES; f++) {
            StringBuilder body = new StringBuilder("id\n");
            for (int i = 0; i < ROWS_PER_FILE; i++) {
                body.append(f * ROWS_PER_FILE + i).append('\n');
            }
            Files.writeString(root.resolve("part_" + f + ".csv"), body.toString(), StandardCharsets.UTF_8);
        }
        String dataset = registerDataset("unreachable_node_" + allowPartial, StoragePath.fileUri(root) + "/*.csv", Map.of());
        String query = "FROM " + dataset + " | STATS c = COUNT(*), s = SUM(id)";

        try (var response = client(coordinator).execute(EsqlQueryAction.INSTANCE, request(query, true)).actionGet(TIMEOUT)) {
            assertThat(getValuesList(response), equalTo(List.of(List.of(ROWS, ID_SUM))));
            assertThat("the node made unreachable below must be assigned splits", externalScanNodeNames(response), hasItem(unreachable));
        }

        var coordinatorTransport = MockTransportService.getInstance(coordinator);
        var unreachableAddress = internalCluster().getInstance(TransportService.class, unreachable).boundAddress().publishAddress();
        coordinatorTransport.addGetConnectionBehavior(unreachableAddress, (connectionManager, node) -> {
            throw new NodeNotConnectedException(node, "simulated: node unreachable at dispatch");
        });
        var faulted = request(query, false);
        faulted.allowPartialResults(allowPartial);
        try (var response = client(coordinator).execute(EsqlQueryAction.INSTANCE, faulted).actionGet(TIMEOUT)) {
            assertThat(response.isPartial(), equalTo(false));
            assertThat(getValuesList(response), equalTo(List.of(List.of(ROWS, ID_SUM))));
        } finally {
            coordinatorTransport.clearAllRules();
        }
    }

    private static EsqlQueryRequest request(String query, boolean profile) {
        var request = syncEsqlQueryRequest(query);
        request.pragmas(new QueryPragmas(Settings.builder().put(QueryPragmas.EXTERNAL_DISTRIBUTION.getKey(), "round_robin").build()));
        request.acceptedPragmaRisks(true);
        request.profile(profile);
        return request;
    }

    private static boolean isDataNode(String nodeName) {
        return internalCluster().clusterService(nodeName).localNode().canContainData();
    }
}

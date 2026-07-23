/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.fieldcaps.FieldCapabilitiesRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.View;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.esql.datasources.dataset.PutDatasetAction;
import org.elasticsearch.xpack.esql.datasources.datasource.PutDataSourceAction;
import org.elasticsearch.xpack.esql.session.IndexResolver;
import org.elasticsearch.xpack.esql.view.PutViewAction;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

public class CrossClusterEsqlResolveFieldsActionIT extends AbstractCrossClusterTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins(String clusterAlias) {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins(clusterAlias));
        plugins.add(CrossClusterDatasetIT.TestDataSourcePlugin.class);
        return plugins;
    }

    public void testResolveIndexAbstractions() throws IOException {
        assumeTrue("requires dataset-in-from-command capability", EsqlCapabilities.Cap.DATASET_IN_FROM_COMMAND.isEnabled());

        client(LOCAL_CLUSTER).admin().indices().prepareCreate("local-index").setMapping("f1", "type=keyword", "f2", "type=long").get();
        client(REMOTE_CLUSTER_1).admin().indices().prepareCreate("remote-index").setMapping("f1", "type=keyword", "f3", "type=long").get();

        createView(client(LOCAL_CLUSTER), "local-view", "FROM local-index | WHERE true");
        createView(client(REMOTE_CLUSTER_1), "remote-view", "FROM remote-index | WHERE true");

        createExternalDataset(client(LOCAL_CLUSTER), "local-data-source", "local-data-set");
        createExternalDataset(client(REMOTE_CLUSTER_1), "remote-data-source", "remote-data-set");

        // resolve schema
        var request = new EsqlResolveFieldsRequest(
            new FieldCapabilitiesRequest().indices("*", "*:*").fields("*").indicesOptions(IndexResolver.DEFAULT_OPTIONS),
            false, // permit remote views and data sets
            true // resolve index abstractions
        );
        var response = client(LOCAL_CLUSTER).execute(EsqlResolveFieldsAction.TYPE, request).actionGet(30, TimeUnit.SECONDS);
        assertThat(
            response.resolvedIndexAbstractions(),
            Matchers.containsInAnyOrder(
                new EsqlResolveFieldsResponse.ResolvedIndexAbstraction("local-index", IndexAbstraction.Type.CONCRETE_INDEX),
                new EsqlResolveFieldsResponse.ResolvedIndexAbstraction("cluster-a:remote-index", IndexAbstraction.Type.CONCRETE_INDEX),
                new EsqlResolveFieldsResponse.ResolvedIndexAbstraction("local-view", IndexAbstraction.Type.VIEW),
                new EsqlResolveFieldsResponse.ResolvedIndexAbstraction("cluster-a:remote-view", IndexAbstraction.Type.VIEW),
                new EsqlResolveFieldsResponse.ResolvedIndexAbstraction("local-data-set", IndexAbstraction.Type.DATASET),
                new EsqlResolveFieldsResponse.ResolvedIndexAbstraction("cluster-a:remote-data-set", IndexAbstraction.Type.DATASET)
            )
        );
    }

    private static void createView(Client client, String name, String query) {
        assertAcked(
            client.execute(
                PutViewAction.INSTANCE,
                new PutViewAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, new View(name, query))
            ).actionGet(30, TimeUnit.SECONDS)
        );
    }

    private static void createExternalDataset(Client client, String dataSourceName, String dataSetName) throws IOException {
        Path csvFixture = createTempFile("dataset-", ".csv");
        Files.writeString(csvFixture, "f1:keyword,f4:long\nalice,1\nbob,2\n");

        assertAcked(
            client.execute(
                PutDataSourceAction.INSTANCE,
                new PutDataSourceAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, dataSourceName, "test", null, new HashMap<>())
            ).actionGet(30, TimeUnit.SECONDS)
        );
        assertAcked(
            client.execute(
                PutDatasetAction.INSTANCE,
                new PutDatasetAction.Request(
                    TEST_REQUEST_TIMEOUT,
                    TEST_REQUEST_TIMEOUT,
                    dataSetName,
                    dataSourceName,
                    csvFixture.toUri().toString(),
                    null,
                    Map.of("format", "csv")
                )
            ).actionGet(30, TimeUnit.SECONDS)
        );
    }
}

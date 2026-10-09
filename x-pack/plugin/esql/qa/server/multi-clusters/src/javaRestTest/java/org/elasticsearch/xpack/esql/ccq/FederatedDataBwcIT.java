/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.ccq;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.apache.http.HttpHost;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.esql.AssertWarnings;
import org.elasticsearch.xpack.esql.CsvTestUtils;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.datasources.DatasetRegistry;
import org.elasticsearch.xpack.esql.datasources.EsqlDataSourcesCapabilities;
import org.elasticsearch.xpack.esql.qa.rest.RestEsqlTestCase;
import org.junit.AfterClass;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.xpack.esql.qa.rest.RestEsqlTestCase.hasCapabilities;

/**
 * Verifies a successful union of a local dataset and a remote index across the 9.5+ BWC boundary.
 */
@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class FederatedDataBwcIT extends ESRestTestCase {

    private static final String DATA_SOURCE = "bwc_local_source";
    private static final String DATASET = "bwc_local_dataset";
    private static final String REMOTE_INDEX = "bwc-remote-index";
    private static final Path DATA_PATH = CsvTestUtils.createCsvDataDirectory();

    private static final ElasticsearchCluster remoteCluster = Clusters.remoteCluster(DATA_PATH, emptyMap(), false);
    private static final ElasticsearchCluster localCluster = Clusters.localCluster(DATA_PATH, remoteCluster, false, emptyMap(), false);

    @ClassRule
    public static TestRule clusterRule = RuleChain.outerRule((base, description) -> new org.junit.runners.model.Statement() {
        @Override
        public void evaluate() throws Throwable {
            assumeFalse("FIPS mode requires security enabled; this test uses an unsecured local datasource", inFipsJvm());
            assumeTrue(
                "federated data-source BWC coverage starts at 9.5.0",
                Clusters.bwcVersion().onOrAfter(org.elasticsearch.Version.V_9_5_0)
            );
            base.evaluate();
        }
    }).around(remoteCluster).around(localCluster);

    @Override
    protected String getTestRestCluster() {
        return localCluster.getHttpAddresses();
    }

    @AfterClass
    public static void cleanupDatasets() throws IOException {
        try {
            DatasetRegistry.cleanup(client());
        } finally {
            DatasetRegistry.clearCaches();
        }
    }

    public void testLocalDatasetCombinedWithRemoteIndex() throws Exception {
        assumeTrue(
            "dataset and subquery sources are required on the coordinating cluster",
            hasCapabilities(
                adminClient(),
                List.of(
                    EsqlCapabilities.Cap.DATASET_IN_FROM_COMMAND.capabilityName(),
                    EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.capabilityName()
                )
            )
        );
        assumeTrue(
            "data-source registration is required on the coordinating cluster",
            clusterHasCapability(
                adminClient(),
                "PUT",
                "/_query/data_source/{name}",
                List.of(),
                List.of(EsqlDataSourcesCapabilities.DATA_SOURCES)
            ).orElse(false)
        );

        Path csv = DATA_PATH.resolve("bwc-local-dataset.csv");
        Files.writeString(csv, "id:integer,name:keyword\n1,local-1\n2,local-2\n");
        DatasetRegistry.ensureDataSource(client(), DATA_SOURCE, "local", Map.of());
        DatasetRegistry.ensureDataset(client(), DATASET, DATA_SOURCE, csv.toUri().toString(), null);

        try (RestClient remoteClient = remoteClusterClient()) {
            createRemoteIndex(remoteClient);
            try {
                Map<String, Object> result = RestEsqlTestCase.runEsqlSync(
                    new RestEsqlTestCase.RequestObjectBuilder().query(Strings.format("""
                        FROM (FROM %s | KEEP id, name),
                             (FROM *:%s | KEEP id, name)
                        | SORT id
                        """, DATASET, REMOTE_INDEX)),
                    new AssertWarnings.NoWarnings(),
                    null
                );
                assertEquals(
                    List.of(Map.of("name", "id", "type", "integer"), Map.of("name", "name", "type", "keyword")),
                    result.get("columns")
                );
                assertEquals(
                    List.of(List.of(1, "local-1"), List.of(2, "local-2"), List.of(3, "remote-3"), List.of(4, "remote-4")),
                    result.get("values")
                );
            } finally {
                remoteClient.performRequest(new Request("DELETE", "/" + REMOTE_INDEX));
            }
        }
    }

    private static void createRemoteIndex(RestClient remoteClient) throws IOException {
        Request create = new Request("PUT", "/" + REMOTE_INDEX);
        create.setJsonEntity("""
            {
              "mappings": {
                "properties": {
                  "id": {"type": "integer"},
                  "name": {"type": "keyword"}
                }
              }
            }""");
        assertOK(remoteClient.performRequest(create));
        for (int id = 3; id <= 4; id++) {
            Request index = new Request("PUT", "/" + REMOTE_INDEX + "/_doc/" + id);
            index.setJsonEntity("{\"id\":" + id + ",\"name\":\"remote-" + id + "\"}");
            assertOK(remoteClient.performRequest(index));
        }
        assertOK(remoteClient.performRequest(new Request("POST", "/" + REMOTE_INDEX + "/_refresh")));
    }

    private RestClient remoteClusterClient() throws IOException {
        HttpHost[] remoteHosts = parseClusterHosts(remoteCluster.getHttpAddresses()).toArray(HttpHost[]::new);
        return buildClient(restClientSettings(), remoteHosts);
    }
}

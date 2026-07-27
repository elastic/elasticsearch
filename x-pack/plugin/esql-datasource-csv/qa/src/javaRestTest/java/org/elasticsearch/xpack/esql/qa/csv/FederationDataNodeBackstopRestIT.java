/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.csv;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.Build;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.WarningsHandler;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.test.AzureReactorThreadFilter;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.esql.datasources.DatasetRegistry;
import org.elasticsearch.xpack.esql.datasources.FixtureUtils;
import org.elasticsearch.xpack.esql.datasources.S3FixtureUtils.DataSourcesS3HttpFixture;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.nio.file.Path;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

/**
 * Exercises the federation kill switch's data-node backstop ({@code LocalExecutionPlanner.planExternalSource})
 * at its call site, which the pure-unit {@code FederationTests} cannot: that helper test drives
 * {@code Federation.ensureEnabled(boolean)} directly, so deleting the backstop call from the planner would not
 * make it fail.
 *
 * <p>The cluster splits roles across two JVMs: node 0 is coordinating-only with federation <em>enabled</em>,
 * node 1 is master+data with federation <em>suppressed</em> at boot. This is the mixed / rolling-restart window
 * the backstop guards. All requests target the enabled coordinator, which registers a local-file CSV data
 * source and dataset (the create actions still work through the suppressed master because the transport actions
 * stay registered), then runs {@code FROM <dataset>}. The coordinator resolves the dataset and dispatches the
 * external scan to the disabled data node, which must refuse it at operator build with a {@code 400}
 * ({@code external data sources are not available}) rather than reading the file. With the backstop removed the
 * data node would scan the CSV and the query would succeed, so this test goes red exactly when the wiring is
 * lost.
 */
@ThreadLeakFilters(filters = { TestClustersThreadFilter.class, AzureReactorThreadFilter.class })
public class FederationDataNodeBackstopRestIT extends ESRestTestCase {

    private static final Logger logger = LogManager.getLogger(FederationDataNodeBackstopRestIT.class);

    private static final String LOCAL_DATA_SOURCE = "backstop_csv_ds";
    private static final String DATASET = "backstop_employees";

    public static DataSourcesS3HttpFixture s3Fixture = new DataSourcesS3HttpFixture();

    public static ElasticsearchCluster cluster = Clusters.multiNodeCoordinatorEnabledDataNodeDisabledCluster(() -> s3Fixture.getAddress());

    @ClassRule
    public static TestRule ruleChain = RuleChain.outerRule(s3Fixture).around(cluster);

    private static Path localFixturesPath;

    /** Datasets and {@code local} data sources are gated to snapshot builds today (same gate as {@code DataSourceCrudRestIT}). */
    @BeforeClass
    public static void requireSnapshotBuild() {
        assumeTrue("datasources not available in release builds yet", Build.current().isSnapshot());
    }

    @BeforeClass
    public static void resolveLocalFixtures() {
        localFixturesPath = FixtureUtils.resolveLocalFixturesPath(logger, FederationDataNodeBackstopRestIT.class);
    }

    @AfterClass
    public static void cleanupDatasets() throws Exception {
        try {
            DatasetRegistry.cleanup(client());
        } finally {
            DatasetRegistry.clearCaches();
        }
    }

    @Override
    protected String getTestRestCluster() {
        // Pin every request to node 0, the coordinating-only node that still has federation enabled, so it is
        // the node that resolves FROM <dataset> and dispatches the external scan to the disabled data node.
        return cluster.getHttpAddress(0);
    }

    public void testDisabledDataNodeRefusesExternalScanFromEnabledCoordinator() throws Exception {
        assumeTrue("LOCAL fixtures unavailable (packaged in a JAR)", localFixturesPath != null);
        String uri = localFixturesPath.resolve("standalone/employees.csv").toUri().toString();
        // Register a local-file data source + dataset over the fixture. The create requests go through the
        // suppressed master node: they succeed because the federation transport actions stay registered even
        // when the switch is engaged (only the REST handlers are unregistered).
        DatasetRegistry.ensureDataSource(client(), LOCAL_DATA_SOURCE, "local", Map.of());
        DatasetRegistry.ensureDataset(client(), DATASET, LOCAL_DATA_SOURCE, uri, "{ \"multi_value_syntax\": \"brackets\" }");

        Request request = new Request("POST", "/_query");
        // external_distribution=round_robin forces the scan onto the data node (the adaptive default might run a
        // single small split locally on the coordinator), so the disabled data node's backstop is the path under test.
        request.setJsonEntity(
            "{\"query\":\"FROM " + DATASET + " | STATS c = COUNT(*)\",\"pragma\":{\"external_distribution\":\"round_robin\"}}"
        );
        // ES|QL warns "No limit defined" for a bare aggregate; that warning is not the subject of this test.
        request.setOptions(RequestOptions.DEFAULT.toBuilder().setWarningsHandler(WarningsHandler.PERMISSIVE));

        ResponseException ex = expectThrows(ResponseException.class, () -> client().performRequest(request));
        String body = EntityUtils.toString(ex.getResponse().getEntity());
        assertThat(ex.getResponse().getStatusLine().getStatusCode(), equalTo(400));
        assertThat(body, containsString("external data sources are not available"));
    }
}

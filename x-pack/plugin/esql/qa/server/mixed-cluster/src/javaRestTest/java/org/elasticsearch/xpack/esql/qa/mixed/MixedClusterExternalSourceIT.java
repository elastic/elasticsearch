/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.mixed;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.apache.http.HttpHost;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.ObjectPath;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.datasources.DatasetRegistry;
import org.elasticsearch.xpack.esql.datasources.EsqlDataSourcesCapabilities;
import org.elasticsearch.xpack.esql.datasources.S3FixtureUtils.DataSourcesS3HttpFixture;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.xpack.esql.datasources.S3FixtureUtils.BUCKET;
import static org.elasticsearch.xpack.esql.datasources.S3FixtureUtils.WAREHOUSE;
import static org.elasticsearch.xpack.esql.datasources.S3FixtureUtils.addBlobToFixture;
import static org.elasticsearch.xpack.esql.qa.rest.RestEsqlTestCase.hasCapabilities;

/**
 * Exercises external-source plans and profiles across both directions of a 9.5+ mixed-version cluster.
 */
@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class MixedClusterExternalSourceIT extends ESRestTestCase {

    private static final String DATA_SOURCE = "bwc_s3";
    private static final String PROJECTION_DATASET = "bwc_projection";
    private static final String DISTRIBUTED_AGGREGATE_DATASET = "bwc_distributed_aggregate";
    private static final String CACHE_AGGREGATE_DATASET = "bwc_cache_aggregate";
    private static final String DECLARED_DATASET = "bwc_declared";
    private static final String PROJECTION_PREFIX = WAREHOUSE + "/bwc_projection";
    private static final String AGGREGATE_PREFIX = WAREHOUSE + "/bwc_aggregate";
    private static final String CACHE_AGGREGATE_PREFIX = WAREHOUSE + "/bwc_cache_aggregate";
    private static final String DECLARED_PREFIX = WAREHOUSE + "/bwc_declared";
    private static final Path CSV_DATA_PATH = org.elasticsearch.xpack.esql.CsvTestUtils.createCsvDataDirectory();

    private static final DataSourcesS3HttpFixture s3Fixture = new DataSourcesS3HttpFixture();
    private static final ElasticsearchCluster cluster = Clusters.mixedVersionCluster(CSV_DATA_PATH, false);

    @ClassRule
    public static TestRule ruleChain = RuleChain.outerRule((base, description) -> new org.junit.runners.model.Statement() {
        @Override
        public void evaluate() throws Throwable {
            assumeFalse("FIPS mode requires security enabled; this test uses a plain HTTP S3 fixture", inFipsJvm());
            assumeTrue(
                "external data-source BWC coverage starts at 9.5.0",
                MixedClusterTestSupport.bwcVersion().onOrAfter(org.elasticsearch.Version.V_9_5_0)
            );
            assumeTrue(
                "requires distinguishable old and current nodes",
                MixedClusterTestSupport.bwcVersion().before(org.elasticsearch.Version.CURRENT)
            );
            base.evaluate();
        }
    }).around(s3Fixture).around(cluster);

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @BeforeClass
    public static void uploadCsvFiles() {
        for (String prefix : List.of(PROJECTION_PREFIX, DECLARED_PREFIX)) {
            for (int i = 1; i <= 4; i++) {
                String csv = "id:integer,name:keyword,value:integer\n" + i + ",name-" + i + "," + (i * 10) + "\n";
                addBlobToFixture(s3Fixture.getHandler(), prefix + "/part-" + i + ".csv", csv.getBytes(StandardCharsets.UTF_8));
            }
        }
        for (int part = 0; part < 4; part++) {
            StringBuilder csv = new StringBuilder("id:integer,name:keyword,value:integer\n");
            for (int row = 1; row <= 200; row++) {
                int id = part * 200 + row;
                csv.append(id).append(",name-").append(id).append(',').append(id * 10).append('\n');
            }
            byte[] contents = csv.toString().getBytes(StandardCharsets.UTF_8);
            addBlobToFixture(s3Fixture.getHandler(), AGGREGATE_PREFIX + "/part-" + (part + 1) + ".csv", contents);
            if (part == 0) {
                addBlobToFixture(s3Fixture.getHandler(), CACHE_AGGREGATE_PREFIX + "/part-1.csv", contents);
            }
        }
    }

    @AfterClass
    public static void cleanupDatasets() throws IOException {
        try {
            DatasetRegistry.cleanup(client());
        } finally {
            DatasetRegistry.clearCaches();
        }
    }

    public void testExternalSourceWireCompatibilityInBothDirections() throws Exception {
        assumeTrue(
            "FROM <dataset> is required",
            hasCapabilities(adminClient(), List.of(EsqlCapabilities.Cap.DATASET_IN_FROM_COMMAND.capabilityName()))
        );
        assumeTrue(
            "data-source registration is required",
            clusterHasCapability(
                adminClient(),
                "PUT",
                "/_query/data_source/{name}",
                List.of(),
                List.of(EsqlDataSourcesCapabilities.DATA_SOURCES)
            ).orElse(false)
        );
        assumeTrue(
            "declared dataset schemas are required",
            clusterHasCapability(
                adminClient(),
                "PUT",
                "/_query/dataset/{name}",
                List.of(),
                List.of(EsqlDataSourcesCapabilities.DATASET_DECLARED_SCHEMA)
            ).orElse(false)
        );

        registerDatasets();

        ObjectPath nodesInfo = ObjectPath.createFromResponse(adminClient().performRequest(new Request("GET", "/_nodes")));
        List<MixedClusterTestSupport.Node> oldNodes = MixedClusterTestSupport.nodesForCoordinator(nodesInfo, true);
        List<MixedClusterTestSupport.Node> currentNodes = MixedClusterTestSupport.nodesForCoordinator(nodesInfo, false);

        QueryResult expectedProjection = null;
        QueryResult expectedDistributedAggregate = null;
        QueryResult expectedCacheAggregate = null;
        QueryResult expectedDeclared = null;
        for (MixedClusterTestSupport.Node coordinator : List.of(oldNodes.get(0), currentNodes.get(0))) {
            try (RestClient coordinatorClient = coordinatorClient(coordinator)) {
                Map<String, Object> projection = runQuery(
                    coordinatorClient,
                    "FROM " + PROJECTION_DATASET + " | KEEP id, name, value | SORT id | LIMIT 10",
                    "round_robin"
                );
                QueryResult projectionResult = QueryResult.from(projection);
                assertProjectionResult(projectionResult);
                expectedProjection = assertConsistent(expectedProjection, projectionResult);
                assertDistributedCsvProfile(projection, oldNodes, currentNodes);

                QueryResult coordinatorOnly = QueryResult.from(
                    runQuery(
                        coordinatorClient,
                        "FROM " + PROJECTION_DATASET + " | KEEP id, name, value | SORT id | LIMIT 10",
                        "coordinator_only"
                    )
                );
                assertEquals("distribution modes must return identical columns", projectionResult.columns(), coordinatorOnly.columns());
                assertEquals("distribution modes must return identical rows", projectionResult.values(), coordinatorOnly.values());

                String distributedAggregateQuery = "FROM "
                    + DISTRIBUTED_AGGREGATE_DATASET
                    + " | STATS count = COUNT(*), min = MIN(value), max = MAX(value) | LIMIT 1";
                Map<String, Object> distributedAggregate = runQuery(coordinatorClient, distributedAggregateQuery, "round_robin");
                QueryResult distributedAggregateResult = QueryResult.from(distributedAggregate);
                assertDistributedAggregateResult(distributedAggregateResult);
                expectedDistributedAggregate = assertConsistent(expectedDistributedAggregate, distributedAggregateResult);
                assertColdScan(distributedAggregate);
                assertDistributedCsvProfile(distributedAggregate, oldNodes, currentNodes);

                String cacheAggregateQuery = "FROM " + CACHE_AGGREGATE_DATASET + " | STATS count = COUNT(*) | LIMIT 1";
                Map<String, Object> coldCacheAggregate = runQuery(coordinatorClient, cacheAggregateQuery, "coordinator_only");
                QueryResult cacheAggregateResult = QueryResult.from(coldCacheAggregate);
                assertCacheAggregateResult(cacheAggregateResult);
                expectedCacheAggregate = assertConsistent(expectedCacheAggregate, cacheAggregateResult);
                assertColdScan(coldCacheAggregate);

                // Cache reconciliation can complete asynchronously on distributed paths. Keep every attempt on the
                // concrete coordinator and wait for its profile to affirm that the aggregate was served warm.
                assertBusy(() -> {
                    Map<String, Object> warmAggregate = runQuery(coordinatorClient, cacheAggregateQuery, "coordinator_only");
                    assertEquals(cacheAggregateResult.columns(), QueryResult.from(warmAggregate).columns());
                    assertEquals(cacheAggregateResult.values(), QueryResult.from(warmAggregate).values());
                    assertWarmAggregate(warmAggregate, 1);
                }, 30, TimeUnit.SECONDS);

                QueryResult declared = QueryResult.from(
                    runQuery(
                        coordinatorClient,
                        "FROM " + DECLARED_DATASET + " | KEEP identifier, label | SORT identifier | LIMIT 10",
                        "round_robin"
                    )
                );
                assertDeclaredResult(declared);
                expectedDeclared = assertConsistent(expectedDeclared, declared);
            }
        }
    }

    private static void registerDatasets() throws IOException {
        DatasetRegistry.ensureDataSource(client(), DATA_SOURCE, "s3", Map.of("endpoint", s3Fixture.getAddress(), "auth", "anonymous"));
        DatasetRegistry.ensureDataset(client(), PROJECTION_DATASET, DATA_SOURCE, s3Glob(PROJECTION_PREFIX), null);
        DatasetRegistry.ensureDataset(client(), DISTRIBUTED_AGGREGATE_DATASET, DATA_SOURCE, s3Glob(AGGREGATE_PREFIX), null);
        DatasetRegistry.ensureDataset(
            client(),
            CACHE_AGGREGATE_DATASET,
            DATA_SOURCE,
            "s3://" + BUCKET + "/" + CACHE_AGGREGATE_PREFIX + "/part-1.csv",
            null
        );
        DatasetRegistry.ensureDataset(client(), DECLARED_DATASET, DATA_SOURCE, s3Glob(DECLARED_PREFIX), """
            {
              "mappings": {
                "dynamic": "false",
                "properties": {
                  "identifier": {"type": "integer", "path": "id"},
                  "label": {"type": "keyword", "path": "name"}
                }
              }
            }""");
    }

    private static String s3Glob(String prefix) {
        return "s3://" + BUCKET + "/" + prefix + "/*.csv";
    }

    private RestClient coordinatorClient(MixedClusterTestSupport.Node coordinator) throws IOException {
        return buildClient(restClientSettings(), new HttpHost[] { HttpHost.create(coordinator.httpAddress()) });
    }

    private static Map<String, Object> runQuery(RestClient coordinatorClient, String query, String distribution) throws IOException {
        Request request = new Request("POST", "/_query");
        request.setJsonEntity(Strings.format("""
            {
              "query": "%s",
              "profile": true,
              "accept_pragma_risks": true,
              "pragma": {"external_distribution": "%s"}
            }""", query, distribution));
        return entityAsMap(coordinatorClient.performRequest(request));
    }

    private static QueryResult assertConsistent(QueryResult expected, QueryResult actual) {
        if (expected != null) {
            assertEquals("coordinator versions must return identical columns", expected.columns(), actual.columns());
            assertEquals("coordinator versions must return identical rows", expected.values(), actual.values());
            return expected;
        }
        return actual;
    }

    private static void assertProjectionResult(QueryResult result) {
        assertEquals(
            List.of(
                Map.of("name", "id", "type", "integer"),
                Map.of("name", "name", "type", "keyword"),
                Map.of("name", "value", "type", "integer")
            ),
            result.columns()
        );
        assertEquals(
            List.of(List.of(1, "name-1", 10), List.of(2, "name-2", 20), List.of(3, "name-3", 30), List.of(4, "name-4", 40)),
            result.values()
        );
    }

    private static void assertDistributedAggregateResult(QueryResult result) {
        assertEquals(1, result.values().size());
        assertEquals(800L, ((Number) result.values().get(0).get(0)).longValue());
        assertEquals(10, ((Number) result.values().get(0).get(1)).intValue());
        assertEquals(8000, ((Number) result.values().get(0).get(2)).intValue());
    }

    private static void assertCacheAggregateResult(QueryResult result) {
        assertEquals(1, result.values().size());
        assertEquals(200L, ((Number) result.values().get(0).get(0)).longValue());
    }

    private static void assertDeclaredResult(QueryResult result) {
        assertEquals(
            List.of(Map.of("name", "identifier", "type", "integer"), Map.of("name", "label", "type", "keyword")),
            result.columns()
        );
        assertEquals(List.of(List.of(1, "name-1"), List.of(2, "name-2"), List.of(3, "name-3"), List.of(4, "name-4")), result.values());
    }

    @SuppressWarnings("unchecked")
    private static void assertDistributedCsvProfile(
        Map<String, Object> response,
        List<MixedClusterTestSupport.Node> oldNodes,
        List<MixedClusterTestSupport.Node> currentNodes
    ) {
        Set<String> csvReaderNodes = csvReaderNodes(response);
        assertFalse("profile must contain a CsvReaderStatus", csvReaderNodes.isEmpty());
        assertTrue(
            "round_robin must execute CSV scans on every old node; saw " + csvReaderNodes,
            csvReaderNodes.containsAll(oldNodes.stream().map(MixedClusterTestSupport.Node::name).toList())
        );
        assertTrue(
            "round_robin must execute CSV scans on every current node; saw " + csvReaderNodes,
            csvReaderNodes.containsAll(currentNodes.stream().map(MixedClusterTestSupport.Node::name).toList())
        );
    }

    @SuppressWarnings("unchecked")
    private static Set<String> csvReaderNodes(Map<String, Object> response) {
        Map<String, Object> profile = (Map<String, Object>) response.get("profile");
        assertNotNull("profile is required", profile);
        List<Map<String, Object>> drivers = (List<Map<String, Object>>) profile.get("drivers");
        Set<String> csvReaderNodes = new HashSet<>();
        for (Map<String, Object> driver : drivers) {
            List<Map<String, Object>> operators = (List<Map<String, Object>>) driver.get("operators");
            for (Map<String, Object> operator : operators) {
                Map<String, Object> status = (Map<String, Object>) operator.get("status");
                if (status == null) {
                    continue;
                }
                Map<String, Object> formatReader = (Map<String, Object>) status.get("format_reader");
                if (formatReader != null && "csv".equals(formatReader.get("format"))) {
                    csvReaderNodes.add((String) driver.get("node_name"));
                }
            }
        }
        return csvReaderNodes;
    }

    @SuppressWarnings("unchecked")
    private static void assertColdScan(Map<String, Object> response) {
        Map<String, Object> profile = (Map<String, Object>) response.get("profile");
        assertNotNull("profile is required", profile);
        assertTrue("cold aggregate must scan external splits", ((Number) profile.get("splits_scanned")).intValue() > 0);
        assertNull("cold aggregate must not report a warm short-circuit", profile.get("external_warm_aggregates"));
    }

    @SuppressWarnings("unchecked")
    private static void assertWarmAggregate(Map<String, Object> response, int expectedWarmAggregates) {
        Map<String, Object> profile = (Map<String, Object>) response.get("profile");
        assertNotNull("profile is required", profile);
        Number warm = (Number) profile.get("external_warm_aggregates");
        assertNotNull("warm aggregate must report cached source metadata", warm);
        assertEquals("every aggregate branch must be served from cached metadata", expectedWarmAggregates, warm.intValue());
        assertNull("warm aggregate must not scan external splits", profile.get("splits_scanned"));
    }

    private record QueryResult(List<Map<String, Object>> columns, List<List<Object>> values) {
        @SuppressWarnings("unchecked")
        static QueryResult from(Map<String, Object> response) {
            return new QueryResult((List<Map<String, Object>>) response.get("columns"), (List<List<Object>>) response.get("values"));
        }
    }
}

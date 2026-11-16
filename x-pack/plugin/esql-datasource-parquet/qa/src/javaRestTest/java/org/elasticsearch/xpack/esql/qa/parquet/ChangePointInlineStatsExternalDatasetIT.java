/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.parquet;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.elasticsearch.Build;
import org.elasticsearch.client.Request;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.test.AzureReactorThreadFilter;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.esql.AssertWarnings;
import org.elasticsearch.xpack.esql.datasources.DatasetRegistry;
import org.elasticsearch.xpack.esql.datasources.FixtureUtils;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Pattern;

import static org.elasticsearch.xpack.esql.qa.rest.RestEsqlTestCase.requestObjectBuilder;
import static org.elasticsearch.xpack.esql.qa.rest.RestEsqlTestCase.runEsqlSync;

/**
 * Regression test for https://github.com/elastic/elasticsearch/issues/159358
 * <p>
 * {@code CHANGE_POINT} followed by {@code STATS ... BY field} followed by
 * {@code INLINE STATS COUNT(TO_STRING(field))} causes the physical plan optimizer to lose the
 * type-converted intermediate reference {@code $$field$converted_to$keyword} when an external
 * (parquet) dataset is present in the {@code FROM} clause alongside an ES index.
 * <p>
 * The query returns HTTP 500 {@code illegal_state_exception} with message
 * {@code "optimized incorrectly due to missing references"} before the fix.
 */
@ThreadLeakFilters(filters = { TestClustersThreadFilter.class, AzureReactorThreadFilter.class })
public class ChangePointInlineStatsExternalDatasetIT extends ESRestTestCase {

    private static final Logger logger = LogManager.getLogger(ChangePointInlineStatsExternalDatasetIT.class);

    private static final String DATA_SOURCE = "cp_inline_stats_local_ds";
    private static final String PARQUET_DATASET = "cp_inline_stats_parquet_employees";
    private static final String ES_INDEX = "cp_inline_stats_es_employees";

    @ClassRule
    public static ElasticsearchCluster cluster = Clusters.httpOnlyTestCluster();

    private static Path localFixturesPath;
    private static boolean indexCreated = false;

    @BeforeClass
    public static void requireSnapshotBuild() {
        assumeTrue("datasources not available in release builds yet", Build.current().isSnapshot());
    }

    @BeforeClass
    public static void resolveFixtures() {
        localFixturesPath = FixtureUtils.resolveLocalFixturesPath(logger, ChangePointInlineStatsExternalDatasetIT.class);
        assumeTrue("LOCAL fixtures unavailable (packaged in a JAR)", localFixturesPath != null);
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Before
    public void setup() throws Exception {
        // Register local data source and parquet dataset (idempotent via DatasetRegistry cache).
        String resource = localFixturesPath.resolve("standalone/employees.parquet").toUri().toString();
        DatasetRegistry.ensureDataSource(client(), DATA_SOURCE, "local", Map.of());
        DatasetRegistry.ensureDataset(client(), PARQUET_DATASET, DATA_SOURCE, resource, null);

        // Create the ES mirror index once per suite. The index schema matches the columns used by the
        // query; only the fields the query references need to be present.
        if (indexCreated == false) {
            createIndex(ES_INDEX, Settings.EMPTY, """
                "properties": {
                  "emp_no":     { "type": "integer" },
                  "birth_date": { "type": "date" },
                  "gender":     { "type": "keyword" },
                  "salary":     { "type": "integer" }
                }
                """);
            bulkIndexEmployees();
            indexCreated = true;
        }
    }

    @AfterClass
    public static void cleanup() throws Exception {
        try {
            DatasetRegistry.cleanup(client());
        } finally {
            indexCreated = false;
            DatasetRegistry.clearCaches();
        }
    }

    /**
     * Verifies that {@code CHANGE_POINT + STATS ... BY gender + INLINE STATS COUNT(TO_STRING(gender))}
     * succeeds when the {@code FROM} clause contains an external parquet dataset alongside an ES index.
     * <p>
     * Before the fix the optimizer dropped {@code $$gender$converted_to$keyword} — the
     * type-converted reference that the external-source plan inserts for the grouped-by field — and
     * the query failed with HTTP 500.
     */
    public void testChangePointStatsInlineStatsWithExternalDataset() throws Exception {
        // https://github.com/elastic/elasticsearch/issues/159358
        var result = runEsqlSync(
            requestObjectBuilder().query(
                "FROM "
                    + PARQUET_DATASET
                    + ", "
                    + ES_INDEX
                    + " | CHANGE_POINT emp_no ON birth_date AS cp_low, cp_high"
                    + " | STATS max_salary = MAX(salary) BY gender"
                    + " | INLINE STATS cp_count = COUNT(TO_STRING(gender))"
            ).build(),
            new AssertWarnings.NoWarnings(),
            null
        );
        // If the optimizer bug is present this throws ResponseException with HTTP 500 before reaching here.
        assertNotNull(result);
    }

    /**
     * Same pipeline as {@link #testChangePointStatsInlineStatsWithExternalDataset} but with only the
     * parquet dataset in the FROM clause — no ES index. Confirms the optimizer bug is specific to the
     * mixed external+ES plan path (where schema merging introduces type-converted references) and does
     * not affect pure-external plans where no such conversion is needed.
     */
    public void testChangePointStatsInlineStatsParquetOnly() throws Exception {
        var result = runEsqlSync(
            requestObjectBuilder().query(
                "FROM "
                    + PARQUET_DATASET
                    + " | CHANGE_POINT emp_no ON birth_date AS cp_low, cp_high"
                    + " | STATS max_salary = MAX(salary) BY gender"
                    + " | INLINE STATS cp_count = COUNT(TO_STRING(gender))"
            ).build(),
            new AssertWarnings.AllowedRegexes(
                List.of(
                    Pattern.compile(".*evaluation of \\[CHANGE_POINT.*\\] failed.*", Pattern.DOTALL),
                    Pattern.compile(".*not enough buckets.*", Pattern.DOTALL)
                )
            ),
            null
        );
        assertNotNull(result);
    }

    private void bulkIndexEmployees() throws IOException {
        Request req = new Request("POST", "/" + ES_INDEX + "/_bulk?refresh=true");
        req.setJsonEntity(buildBulkBody());
        assertOK(client().performRequest(req));
    }

    /**
     * A handful of employees covering both genders and a spread of birth dates so CHANGE_POINT
     * has enough data points to run without tripping on an empty or degenerate input.
     */
    private static String buildBulkBody() {
        // Fields: emp_no, birth_date (ISO-8601), gender (M/F), salary
        Object[][] rows = {
            { 10001, "1953-09-02", "M", 57305 },
            { 10002, "1964-06-02", "F", 56371 },
            { 10003, "1959-12-03", "M", 61805 },
            { 10004, "1954-05-01", "M", 36174 },
            { 10005, "1955-01-21", "M", 63528 },
            { 10006, "1953-04-20", "F", 40006 },
            { 10007, "1957-05-23", "F", 56724 },
            { 10008, "1958-02-19", "M", 46671 },
            { 10009, "1952-04-19", "F", 52765 },
            { 10010, "1963-06-01", "F", 39728 }, };
        StringBuilder sb = new StringBuilder();
        for (Object[] row : rows) {
            sb.append("{\"index\":{}}\n");
            sb.append(
                String.format(
                    Locale.ROOT,
                    "{\"emp_no\":%d,\"birth_date\":\"%s\",\"gender\":\"%s\",\"salary\":%d}\n",
                    row[0],
                    row[1],
                    row[2],
                    row[3]
                )
            );
        }
        return sb.toString();
    }
}

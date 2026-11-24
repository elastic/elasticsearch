/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.parquet;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.elasticsearch.Build;
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

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.qa.rest.RestEsqlTestCase.requestObjectBuilder;
import static org.elasticsearch.xpack.esql.qa.rest.RestEsqlTestCase.runEsqlSync;

/**
 * A pushed Parquet filter over a DOUBLE column must return the same ids as the same
 * predicate evaluated after {@code d * 1.0}, which defeats pushdown so FilterExec uses
 * ES|QL IEEE operators. The fixture is the four-row signed-zero file ({@code -0.0},
 * {@code 0.0}, {@code 1.0}, {@code -1.0}); {@code d * 1.0} throws on NaN, so NaN stays
 * in the unit suite.
 */
@ThreadLeakFilters(filters = { TestClustersThreadFilter.class, AzureReactorThreadFilter.class })
public class ParquetSignedZeroNaNFilterIT extends ESRestTestCase {

    private static final Logger logger = LogManager.getLogger(ParquetSignedZeroNaNFilterIT.class);

    private static final String DATA_SOURCE = "signed_zero_ds";
    private static final String DATASET = "signed_zero";
    private static final String FIXTURE = "signed_zero.parquet";

    @ClassRule
    public static ElasticsearchCluster cluster = Clusters.httpOnlyTestCluster();

    private static Path localFixturesPath;

    @BeforeClass
    public static void requireSnapshotBuild() {
        assumeTrue("datasources not available in release builds yet", Build.current().isSnapshot());
    }

    @BeforeClass
    public static void writeFixture() throws Exception {
        localFixturesPath = FixtureUtils.resolveLocalFixturesPath(logger, ParquetSignedZeroNaNFilterIT.class);
        assumeTrue("LOCAL fixtures unavailable (packaged in a JAR)", localFixturesPath != null);
        Files.write(localFixturesPath.resolve(FIXTURE), EmployeesParquetGenerator.signedZeroParquetBytes());
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
        return cluster.getHttpAddresses();
    }

    @Before
    public void registerDataset() throws Exception {
        String uri = localFixturesPath.resolve(FIXTURE).toUri().toString();
        DatasetRegistry.ensureDataSource(client(), DATA_SOURCE, "local", Map.of());
        DatasetRegistry.ensureDataset(client(), DATASET, DATA_SOURCE, uri, null);
    }

    public void testPushedFilterMatchesIeeeEval() throws Exception {
        for (String pred : List.of(
            "d == 0.0",
            "d == -0.0",
            "d != 0.0",
            "d >= 0.0",
            "d <= -0.0",
            "d > 0.0",
            "d < 0.0",
            "NOT (d > 0.0)",
            "NOT (d >= 0.0)",
            "NOT (d == 0.0)",
            "d IN (0.0, 5.0)",
            "NOT (d IN (0.0, 5.0))",
            "d != 5.0"
        )) {
            String pushed = "FROM " + DATASET + " | WHERE " + pred + " | KEEP id | SORT id";
            String ieee = "FROM " + DATASET + " | WHERE " + pred.replaceAll("\\bd\\b", "(d * 1.0)") + " | KEEP id | SORT id";
            assertEquals(pred, idsOf(ieee), idsOf(pushed));
        }
    }

    @SuppressWarnings("unchecked")
    private List<Object> idsOf(String query) throws Exception {
        Map<String, Object> response = runEsqlSync(requestObjectBuilder().query(query).build(), new AssertWarnings.NoWarnings(), null);
        List<List<Object>> values = (List<List<Object>>) response.get("values");
        List<Object> ids = new ArrayList<>(values.size());
        for (List<Object> row : values) {
            ids.add(row.get(0));
        }
        return ids;
    }
}

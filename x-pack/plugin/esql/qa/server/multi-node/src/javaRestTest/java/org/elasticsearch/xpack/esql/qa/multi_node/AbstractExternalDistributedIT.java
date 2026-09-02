/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.multi_node;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.esql.AssertWarnings;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.datasources.DatasetRegistry;
import org.elasticsearch.xpack.esql.datasources.S3FixtureUtils.DataSourcesS3HttpFixture;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.elasticsearch.xpack.esql.qa.rest.RestEsqlTestCase;
import org.junit.AfterClass;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.datasources.S3FixtureUtils.ACCESS_KEY;
import static org.elasticsearch.xpack.esql.datasources.S3FixtureUtils.BUCKET;
import static org.elasticsearch.xpack.esql.datasources.S3FixtureUtils.SECRET_KEY;

/**
 * Base class for external source distributed integration tests that use the in-memory
 * S3 fixture. Provides a properly-ordered rule chain (fixture then cluster), the REST
 * cluster address, and helpers for building {@code FROM <dataset>} queries (backed by an
 * {@code s3} data source registered over the fixture) and running them in each distribution mode.
 * <p>
 * Extends {@link ESRestTestCase} directly (not {@link RestEsqlTestCase}) to avoid
 * inheriting dozens of unrelated parameterised ESQL REST tests that would run against
 * the S3-backed cluster.
 */
public abstract class AbstractExternalDistributedIT extends ESRestTestCase {

    protected static final List<String> DISTRIBUTION_MODES = List.of("coordinator_only", "round_robin", "adaptive");

    /** The single {@code s3} data source every dataset binds to; registered lazily on first {@link #fromS3}. */
    private static final String S3_DATA_SOURCE = "distributed_s3_ds";

    public static DataSourcesS3HttpFixture s3Fixture = new DataSourcesS3HttpFixture();

    private static ElasticsearchCluster clusterInstance = ExternalDistributedClusters.testCluster(() -> s3Fixture.getAddress());

    @ClassRule
    public static TestRule ruleChain = RuleChain.outerRule((base, description) -> new org.junit.runners.model.Statement() {
        @Override
        public void evaluate() throws Throwable {
            assumeFalse("FIPS mode requires security enabled; this test uses plain HTTP S3 fixtures", inFipsJvm());
            assumeTrue(
                "FROM <dataset> over external data sources required; skip in release builds",
                EsqlCapabilities.Cap.DATASET_IN_FROM_COMMAND.isEnabled()
            );
            base.evaluate();
        }
    }).around(s3Fixture).around(clusterInstance);

    @Override
    protected String getTestRestCluster() {
        return clusterInstance.getHttpAddresses();
    }

    @AfterClass
    public static void cleanupDatasets() throws IOException {
        try {
            DatasetRegistry.cleanup(client());
        } finally {
            DatasetRegistry.clearCaches();
        }
    }

    /**
     * Registers a dataset over the given S3 path (or glob) under the shared {@code s3} data source — creating
     * the data source on first use — and returns a {@code FROM <dataset>} snippet reading it. The dataset name
     * is derived deterministically from the path, so repeated calls for the same resource reuse one dataset
     * while distinct paths (e.g. per-test unique prefixes) get their own. Globs are accepted at registration
     * time (the resource scheme is the only thing validated) and expand the same way the EXTERNAL command did.
     */
    protected String fromS3(String s3Path) throws IOException {
        DatasetRegistry.ensureDataSource(
            client(),
            S3_DATA_SOURCE,
            "s3",
            Map.of("endpoint", s3Fixture.getAddress(), "access_key", ACCESS_KEY, "secret_key", SECRET_KEY)
        );
        String dataset = DatasetRegistry.sanitizeDatasetName("ds_", s3Path);
        DatasetRegistry.ensureDataset(client(), dataset, S3_DATA_SOURCE, "s3://" + BUCKET + "/" + s3Path, null);
        return "FROM " + dataset;
    }

    /**
     * Run an ESQL query synchronously with the given distribution mode pragma.
     */
    protected Map<String, Object> runQueryWithMode(String query, String distributionMode) throws IOException {
        Settings pragmas = Settings.builder().put(QueryPragmas.EXTERNAL_DISTRIBUTION.getKey(), distributionMode).build();
        var request = new RestEsqlTestCase.RequestObjectBuilder().query(query).pragmasOk().pragmas(pragmas);
        return RestEsqlTestCase.runEsqlSync(request, new AssertWarnings.NoWarnings(), null);
    }

    /**
     * Run a query with an explicit distribution mode and intra-file parsing parallelism. A
     * {@code parsingParallelism >= 2} on a file larger than twice the reader's minimum segment size
     * splits the file into multiple byte-range segments, engaging the multi-segment path of
     * {@link org.elasticsearch.xpack.esql.datasources.ParallelParsingCoordinator} rather than the
     * single-stream fallback.
     */
    protected Map<String, Object> runQueryWithMode(String query, String distributionMode, int parsingParallelism) throws IOException {
        Settings pragmas = Settings.builder()
            .put(QueryPragmas.EXTERNAL_DISTRIBUTION.getKey(), distributionMode)
            .put(QueryPragmas.PARSING_PARALLELISM.getKey(), parsingParallelism)
            .build();
        var request = new RestEsqlTestCase.RequestObjectBuilder().query(query).pragmasOk().pragmas(pragmas);
        return RestEsqlTestCase.runEsqlSync(request, new AssertWarnings.NoWarnings(), null);
    }
}

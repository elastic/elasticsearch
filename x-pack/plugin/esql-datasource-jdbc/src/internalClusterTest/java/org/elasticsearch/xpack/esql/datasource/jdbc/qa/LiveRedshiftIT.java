/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.jdbc.qa;

import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.esql.action.AbstractExternalDataSourceIT;
import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;
import org.elasticsearch.xpack.esql.datasource.jdbc.JdbcDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.dataset.DeleteDatasetAction;
import org.elasticsearch.xpack.esql.datasources.dataset.PutDatasetAction;
import org.elasticsearch.xpack.esql.datasources.datasource.DeleteDataSourceAction;
import org.elasticsearch.xpack.esql.datasources.datasource.PutDataSourceAction;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

/**
 * Opt-in <b>live-endpoint</b> smoke test for the JDBC connector against a REAL Amazon Redshift (or Redshift Serverless)
 * cluster. Unlike every other suite in this module it needs neither H2 nor
 * a testcontainer: it drives the connector end-to-end against an externally-provisioned cloud endpoint supplied via
 * system properties, so it can only run where such an endpoint (and its credentials/IAM) exist. It is therefore
 * <b>opt-in and skips cleanly</b> — via {@code assumeTrue} — whenever the endpoint is not configured, which is the case
 * on ordinary CI/dev nodes (including the authoring VM) and in the standard {@code internalClusterTest} run.
 *
 * <h2>What it proves when enabled</h2>
 * It registers a {@code test} data source, registers one JDBC dataset pointing at the live Redshift table, and runs a
 * trivial {@code FROM <dataset> | LIMIT 1}, asserting the query resolves and returns at least one row. That single
 * round-trip is enough to prove the whole production path — {@code SsrfGuard} allowlist ({@code jdbc:redshift://}) →
 * {@code DialectRegistry} → {@code RedshiftDialect} → the user-supplied Redshift JDBC driver → the cloud endpoint —
 * works against a genuine cluster (something {@link RedshiftDialectStandinIT}, which only stands Redshift in with a
 * Postgres backend, cannot do). It deliberately does NOT run the full correctness matrix: a live cloud cluster is not
 * a place to assert exact row values, only that the connector reaches it.
 *
 * <h2>How to point it at a real Redshift / IAM endpoint</h2>
 * The user must drop a Redshift JDBC driver (e.g. {@code com.amazon.redshift:redshift-jdbc42}) on the node's driver
 * path (production: the plugin's {@code drivers/} subdir; here the test-scoped driver already on the
 * {@code internalClusterTest} classpath supplies the {@code jdbc:redshift://}-accepting {@code ServiceLoader} entry),
 * then pass the endpoint via system properties to the Gradle task, e.g.:
 * <pre>{@code
 * ./gradlew :x-pack:plugin:esql-datasource-jdbc:internalClusterTest \
 *   --tests '*LiveRedshiftIT' \
 *   -Dtests.jdbc.redshift.url='jdbc:redshift://my-cluster.abc123.eu-west-1.redshift.amazonaws.com:5439/dev' \
 *   -Dtests.jdbc.redshift.table='public.my_table' \
 *   -Dtests.jdbc.redshift.user='analyst' \
 *   -Dtests.jdbc.redshift.password='<secret>'
 * }</pre>
 * For IAM / temporary-credential auth, omit the password and instead pass the driver's IAM knobs through the connector's
 * {@code connection_properties} passthrough (the same allow-listed channel the other suites use), e.g.:
 * <pre>{@code
 *   -Dtests.jdbc.redshift.url='jdbc:redshift://my-workgroup.123456789012.eu-west-1.redshift-serverless.amazonaws.com:5439/dev' \
 *   -Dtests.jdbc.redshift.table='public.my_table' \
 *   -Dtests.jdbc.redshift.user='IAM:analyst' \
 *   -Dtests.jdbc.redshift.connection_properties='iam=1;region=eu-west-1;AccessKeyID=<AKIA...>;SecretAccessKey=<secret>'
 * }</pre>
 * Only {@code url} and {@code table} are required; {@code user}, {@code password}, and {@code connection_properties}
 * are optional and forwarded verbatim as JDBC dataset {@code WITH} options (so {@code connection_properties} is still
 * subject to the connector's allow-list, credential, and footgun validation). The endpoint's host must NOT be loopback
 * (a real Redshift host never is), so the default {@code SsrfGuard} accepts it without any loopback opt-in.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = false)
public class LiveRedshiftIT extends AbstractExternalDataSourceIT {

    private static final TimeValue TIMEOUT = TimeValue.timeValueSeconds(60);
    private static final String DATASOURCE_NAME = "live_redshift_ds";
    private static final String DATASET_NAME = "live_redshift";

    /** Required: the {@code jdbc:redshift://host:port/db} URL of the live cluster. Absent -> the suite skips. */
    private static final String URL = System.getProperty("tests.jdbc.redshift.url");
    /** Required alongside {@link #URL}: the table (optionally schema-qualified) the trivial query reads from. */
    private static final String TABLE = System.getProperty("tests.jdbc.redshift.table");
    private static final String USER = System.getProperty("tests.jdbc.redshift.user");
    private static final String PASSWORD = System.getProperty("tests.jdbc.redshift.password");
    private static final String CONNECTION_PROPERTIES = System.getProperty("tests.jdbc.redshift.connection_properties");

    /**
     * The JDBC connector is the only format plugin this suite adds; {@link AbstractExternalDataSourceIT} installs the
     * pass-through {@code TestDataSourcePlugin} (type {@code test}), so no JDBC-specific validator is needed — connector
     * lookup keys off the {@code jdbc:} URL scheme, not the data-source type.
     */
    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(JdbcDataSourcePlugin.class);
    }

    /**
     * Registers the live Redshift dataset (when configured) and runs {@code FROM <dataset> | LIMIT 1}. Skips cleanly
     * (assumption not met) when the feature flag is off or no {@code -Dtests.jdbc.redshift.url}/{@code .table} is set,
     * which is the case on a typical dev machine and normal CI — the whole point of an opt-in suite.
     */
    public void testLiveRedshiftEndpointReturnsRow() throws Exception {
        // The feature-flag / capability gate is applied by AbstractExternalDataSourceIT#requireFeatureFlag(); this
        // this suite additionally skips unless a live endpoint is configured.
        assumeTrue(
            "live-endpoint test: set -Dtests.jdbc.redshift.url and -Dtests.jdbc.redshift.table to run against a real Redshift cluster",
            URL != null && URL.isBlank() == false && TABLE != null && TABLE.isBlank() == false
        );

        registerDataSource();
        Map<String, Object> withConfig = new HashMap<>();
        withConfig.put("table", TABLE);
        if (USER != null) {
            withConfig.put("user", USER);
        }
        if (PASSWORD != null) {
            withConfig.put("password", PASSWORD);
        }
        if (CONNECTION_PROPERTIES != null) {
            withConfig.put("connection_properties", CONNECTION_PROPERTIES);
        }
        try {
            assertAcked(
                client().execute(
                    PutDatasetAction.INSTANCE,
                    new PutDatasetAction.Request(TIMEOUT, TIMEOUT, DATASET_NAME, DATASOURCE_NAME, URL, null, withConfig)
                )
            );
            try (EsqlQueryResponse response = run("FROM " + DATASET_NAME + " | LIMIT 1", TIMEOUT)) {
                assertThat(
                    "live Redshift endpoint must return at least one row for FROM " + DATASET_NAME + " | LIMIT 1",
                    getValuesList(response).size(),
                    greaterThanOrEqualTo(1)
                );
            }
        } finally {
            deleteQuietly(
                () -> client().execute(
                    DeleteDatasetAction.INSTANCE,
                    new DeleteDatasetAction.Request(TIMEOUT, TIMEOUT, new String[] { DATASET_NAME })
                ).get(30, TimeUnit.SECONDS)
            );
            deleteQuietly(
                () -> client().execute(
                    DeleteDataSourceAction.INSTANCE,
                    new DeleteDataSourceAction.Request(TIMEOUT, TIMEOUT, new String[] { DATASOURCE_NAME })
                ).get(30, TimeUnit.SECONDS)
            );
        }
    }

    private void registerDataSource() {
        assertAcked(
            client().execute(
                PutDataSourceAction.INSTANCE,
                new PutDataSourceAction.Request(TIMEOUT, TIMEOUT, DATASOURCE_NAME, "test", null, new HashMap<>(Map.of()))
            )
        );
    }

    private interface CleanupStep {
        void run() throws Exception;
    }

    private void deleteQuietly(CleanupStep step) {
        try {
            step.run();
        } catch (Exception e) {
            logger.warn("cleanup step failed", e);
        }
    }
}

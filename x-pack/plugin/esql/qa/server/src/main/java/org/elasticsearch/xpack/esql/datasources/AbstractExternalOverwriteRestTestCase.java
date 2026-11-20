/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.LocalClusterSpecBuilder;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.esql.AssertWarnings;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.qa.rest.RestEsqlTestCase;
import org.junit.AfterClass;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runners.model.Statement;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.datasources.S3FixtureUtils.WAREHOUSE;

/**
 * Shared ground for REST tests that overwrite an external Parquet object in place and query it again: the cluster and
 * rules every such class needs, dataset registration through a {@link BackendFixture}, the overwrite fixtures, and a query
 * that returns long values. A subclass supplies the backend and its own cluster, since both are static per class.
 */
public abstract class AbstractExternalOverwriteRestTestCase extends ESRestTestCase {

    /** 100-row files, under the 64 KiB footer prefetch: a query reads each whole, so its rows are served from the footer cache. */
    protected static final String SMALL_V1 = "/same_length_overwrite/v1.parquet";
    protected static final String SMALL_V2 = "/same_length_overwrite/v2.parquet";
    /** 10000-row files, whose column data is above the footer prefetch. In each second file the values start at 1000000. */
    protected static final String LARGE_V1 = "/same_length_overwrite/large_v1.parquet";
    protected static final String LARGE_V2 = "/same_length_overwrite/large_v2.parquet";
    /** {@link #LARGE_V2} with one more row, so a different length from {@link #LARGE_V1}. */
    protected static final String LONGER_V2 = "/same_length_overwrite/longer_v2.parquet";

    private static final String ENCRYPTION_PASSWORD_ID = "test";

    /** The store the datasets read from. */
    protected abstract BackendFixture backend();

    /** A cluster that can query external datasets, with the given listing TTL. The caller adds what its backend needs. */
    protected static LocalClusterSpecBuilder<ElasticsearchCluster> externalDatasetCluster(String listingTtl) {
        return ElasticsearchCluster.local()
            .distribution(DistributionType.DEFAULT)
            .setting(S3FixtureUtils.ALLOWED_ENDPOINT_HOSTS_SETTING, S3FixtureUtils.LOOPBACK_ENDPOINT_HOSTS)
            .setting("xpack.security.enabled", "false")
            .setting("xpack.license.self_generated.type", "trial")
            .setting(Federation.FEDERATION_ENABLED.getKey(), "true")
            .setting("esql.external.cache.listing.ttl", listingTtl)
            .keystore("cluster.state.encryption.password." + ENCRYPTION_PASSWORD_ID, "same-length-overwrite-password")
            .keystore("cluster.state.encryption.active_password_id", ENCRYPTION_PASSWORD_ID);
    }

    /** The fixture, then the cluster, skipped in FIPS mode (the fixtures are plain HTTP) and where datasets are not enabled. */
    protected static TestRule externalDatasetRules(String backendName, TestRule fixture, ElasticsearchCluster cluster) {
        return RuleChain.outerRule((base, description) -> new Statement() {
            @Override
            public void evaluate() throws Throwable {
                assumeFalse("FIPS mode requires security enabled; this test uses plain HTTP " + backendName + " fixtures", inFipsJvm());
                assumeTrue("FROM <dataset> over external data sources required", EsqlCapabilities.Cap.DATASET_IN_FROM_COMMAND.isEnabled());
                base.evaluate();
            }
        }).around(fixture).around(cluster);
    }

    @AfterClass
    public static void cleanupDatasets() throws IOException {
        try {
            DatasetRegistry.cleanup(client());
        } finally {
            DatasetRegistry.clearCaches();
        }
    }

    /** A key under this backend's own prefix. */
    protected String key(String relative) {
        return WAREHOUSE + "/same_length/" + backend().name() + "/" + relative;
    }

    /** Registers {@code dataset} over {@code key}, which may be a glob, with the dataset options in {@code withJson}. */
    protected void register(String dataset, String key, @Nullable String withJson) throws IOException {
        String dataSource = "same_length_" + backend().name() + "_ds";
        DatasetRegistry.ensureDataSource(client(), dataSource, backend().dataSourceType(), backend().dataSourceSettings());
        DatasetRegistry.ensureDataset(client(), dataset, dataSource, backend().resourceUri(key), withJson);
    }

    protected void upload(String key, String resourceName) throws IOException {
        backend().uploadBlob(key, resource(resourceName));
    }

    private static byte[] resource(String name) throws IOException {
        try (InputStream in = AbstractExternalOverwriteRestTestCase.class.getResourceAsStream(name)) {
            assertNotNull("missing test resource " + name, in);
            return in.readAllBytes();
        }
    }

    /** Runs {@code esql} and returns its rows, each value as a long. */
    @SuppressWarnings("unchecked")
    protected static List<List<Long>> query(String esql) throws IOException {
        RestEsqlTestCase.RequestObjectBuilder req = new RestEsqlTestCase.RequestObjectBuilder().query(esql).pragmasOk();
        Map<String, Object> result = RestEsqlTestCase.runEsqlSync(req, new AssertWarnings.NoWarnings(), null);
        List<List<Object>> values = (List<List<Object>>) result.get("values");
        assertNotNull("Expected values in the ES|QL result", values);
        List<List<Long>> rows = new ArrayList<>();
        for (List<Object> row : values) {
            List<Long> longs = new ArrayList<>();
            for (Object cell : row) {
                longs.add(((Number) cell).longValue());
            }
            rows.add(longs);
        }
        return rows;
    }
}

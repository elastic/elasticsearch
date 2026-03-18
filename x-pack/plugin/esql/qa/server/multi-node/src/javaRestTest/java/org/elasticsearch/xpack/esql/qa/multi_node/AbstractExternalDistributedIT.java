/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.multi_node;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.esql.AssertWarnings;
import org.elasticsearch.xpack.esql.datasources.S3FixtureUtils.DataSourcesS3HttpFixture;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.elasticsearch.xpack.esql.qa.rest.RestEsqlTestCase;
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
 * cluster address, and helpers for building EXTERNAL queries and running them in each
 * distribution mode.
 * <p>
 * Extends {@link ESRestTestCase} directly (not {@link RestEsqlTestCase}) to avoid
 * inheriting dozens of unrelated parameterised ESQL REST tests that would run against
 * the S3-backed cluster.
 */
public abstract class AbstractExternalDistributedIT extends ESRestTestCase {

    protected static final List<String> DISTRIBUTION_MODES = List.of("coordinator_only", "round_robin", "adaptive");

    public static DataSourcesS3HttpFixture s3Fixture = new DataSourcesS3HttpFixture();

    private static ElasticsearchCluster clusterInstance = ExternalDistributedClusters.testCluster(() -> s3Fixture.getAddress());

    @ClassRule
    public static TestRule ruleChain = RuleChain.outerRule((base, description) -> new org.junit.runners.model.Statement() {
        @Override
        public void evaluate() throws Throwable {
            assumeFalse("FIPS mode requires security enabled; this test uses plain HTTP S3 fixtures", inFipsJvm());
            base.evaluate();
        }
    }).around(s3Fixture).around(clusterInstance);

    @Override
    protected String getTestRestCluster() {
        return clusterInstance.getHttpAddresses();
    }

    /**
     * Build an EXTERNAL query targeting the given S3 path, with credentials pointing at the
     * in-memory S3 fixture.
     */
    protected String externalS3Query(String s3Path) {
        return Strings.format(
            """
                EXTERNAL "s3://%s/%s" WITH { "endpoint": "%s", "access_key": "%s", "secret_key": "%s" }""",
            BUCKET,
            s3Path,
            s3Fixture.getAddress(),
            ACCESS_KEY,
            SECRET_KEY
        );
    }

    /**
     * Run an ESQL query synchronously with the given distribution mode pragma.
     */
    protected Map<String, Object> runQueryWithMode(String query, String distributionMode) throws IOException {
        Settings pragmas = Settings.builder().put(QueryPragmas.EXTERNAL_DISTRIBUTION.getKey(), distributionMode).build();
        var request = new RestEsqlTestCase.RequestObjectBuilder().query(query).pragmasOk().pragmas(pragmas);
        return RestEsqlTestCase.runEsqlSync(request, new AssertWarnings.NoWarnings(), null);
    }
}

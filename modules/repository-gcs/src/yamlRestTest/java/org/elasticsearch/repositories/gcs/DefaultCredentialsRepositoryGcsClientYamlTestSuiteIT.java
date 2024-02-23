/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.repositories.gcs;

import fixture.gcs.GoogleCloudStorageHttpFixture;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.core.Booleans;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.elasticsearch.test.rest.yaml.ESClientYamlSuiteTestCase;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

public class DefaultCredentialsRepositoryGcsClientYamlTestSuiteIT extends ESClientYamlSuiteTestCase {
    private static final boolean USE_FIXTURE = Booleans.parseBoolean(System.getProperty("test.google.fixture", "true"));

    private static GoogleCloudStorageHttpFixture fixture = new GoogleCloudStorageHttpFixture(
        true,
        "bucket",
        "computeMetadata/v1/instance/service-accounts/default/token"
    );

    private static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .module("repository-gcs")
        .setting("gcs.client.integration_test.endpoint", () -> fixture.getAddress())
        .environment("GCE_METADATA_HOST", () -> fixture.getAddress().replace("http://", ""))
        .build();

    @ClassRule
    public static TestRule ruleChain = RuleChain.outerRule(fixture).around(cluster);

    public DefaultCredentialsRepositoryGcsClientYamlTestSuiteIT(@Name("yaml") ClientYamlTestCandidate testCandidate) {
        super(testCandidate);
    }

    @BeforeClass
    public static void checkFixtureEnabled() {
        assumeTrue("Only run against test fixture", USE_FIXTURE);
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws Exception {
        return createParameters();
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.repositories.gcs;

import fixture.gcs.GoogleCloudStorageHttpFixture;
import fixture.gcs.TestUtils;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.core.Booleans;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.LocalClusterConfigProvider;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.elasticsearch.test.rest.yaml.ESClientYamlSuiteTestCase;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.nio.charset.StandardCharsets;

public class RepositoryGcsClientYamlTestSuiteIT extends ESClientYamlSuiteTestCase {
    private static final boolean USE_FIXTURE = Booleans.parseBoolean(System.getProperty("test.google.fixture", "true"));

    private static GoogleCloudStorageHttpFixture fixture = new GoogleCloudStorageHttpFixture(USE_FIXTURE, "bucket", "o/oauth2/token");

    protected static LocalClusterConfigProvider clusterConfig = b -> {};

    private static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .module("repository-gcs")
        .setting("gcs.client.integration_test.endpoint", () -> fixture.getAddress(), s -> USE_FIXTURE)
        .setting("gcs.client.integration_test.token_uri", () -> fixture.getAddress() + "/o/oauth2/token", s -> USE_FIXTURE)
        .apply(c -> {
            if (USE_FIXTURE) {
                c.keystore(
                    "gcs.client.integration_test.credentials_file",
                    Resource.fromString(() -> new String(TestUtils.createServiceAccount(random()), StandardCharsets.UTF_8))
                );
            } else {
                c.keystore(
                    "gcs.client.integration_test.credentials_file",
                    Resource.fromFile(PathUtils.get(System.getProperty("test.google.account")))
                );
            }
        })
        .apply(() -> clusterConfig)
        .build();

    @ClassRule
    public static TestRule ruleChain = RuleChain.outerRule(fixture).around(cluster);

    public RepositoryGcsClientYamlTestSuiteIT(@Name("yaml") ClientYamlTestCandidate testCandidate) {
        super(testCandidate);
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

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.repositories.metering.s3;

import fixture.s3.S3HttpFixture;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.xpack.repositories.metering.AbstractRepositoriesMeteringAPIRestTestCase;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.util.List;
import java.util.Map;

public class S3RepositoriesMeteringIT extends AbstractRepositoriesMeteringAPIRestTestCase {

    static final boolean USE_FIXTURE = Boolean.parseBoolean(System.getProperty("tests.use.fixture", "true"));

    public static final S3HttpFixture s3Fixture = new S3HttpFixture(USE_FIXTURE);

    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .keystore("s3.client.repositories_metering.access_key", System.getProperty("s3AccessKey"))
        .keystore("s3.client.repositories_metering.secret_key", System.getProperty("s3SecretKey"))
        .setting("xpack.license.self_generated.type", "trial")
        .setting("s3.client.repositories_metering.protocol", () -> "http", (n) -> USE_FIXTURE)
        .setting("s3.client.repositories_metering.endpoint", s3Fixture::getAddress, (n) -> USE_FIXTURE)
        .setting("xpack.security.enabled", "false")
        .build();

    @ClassRule
    public static TestRule ruleChain = RuleChain.outerRule(s3Fixture).around(cluster);

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected String repositoryType() {
        return "s3";
    }

    @Override
    protected Map<String, String> repositoryLocation() {
        return Map.of("bucket", getProperty("test.s3.bucket"), "base_path", getProperty("test.s3.base_path"));
    }

    @Override
    protected Settings repositorySettings() {
        final String bucket = getProperty("test.s3.bucket");
        final String basePath = getProperty("test.s3.base_path");

        return Settings.builder().put("client", "repositories_metering").put("bucket", bucket).put("base_path", basePath).build();
    }

    @Override
    protected Settings updatedRepositorySettings() {
        Settings settings = repositorySettings();
        return Settings.builder().put(settings).put("s3.client.max_retries", 4).build();
    }

    @Override
    protected List<String> readCounterKeys() {
        return List.of("GetObject", "ListObjects");
    }

    @Override
    protected List<String> writeCounterKeys() {
        return List.of("PutObject");
    }
}

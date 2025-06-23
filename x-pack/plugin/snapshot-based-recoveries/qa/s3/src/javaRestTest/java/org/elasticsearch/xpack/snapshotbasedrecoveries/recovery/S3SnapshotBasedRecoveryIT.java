/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.snapshotbasedrecoveries.recovery;

import fixture.s3.S3HttpFixture;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import static org.hamcrest.Matchers.blankOrNullString;
import static org.hamcrest.Matchers.not;

public class S3SnapshotBasedRecoveryIT extends AbstractSnapshotBasedRecoveryRestTestCase {

    static final boolean USE_FIXTURE = Boolean.parseBoolean(System.getProperty("tests.use.fixture", "true"));

    public static final S3HttpFixture s3Fixture = new S3HttpFixture(USE_FIXTURE);

    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .nodes(3)
        .keystore("s3.client.snapshot_based_recoveries.access_key", System.getProperty("s3AccessKey"))
        .keystore("s3.client.snapshot_based_recoveries.secret_key", System.getProperty("s3SecretKey"))
        .setting("xpack.license.self_generated.type", "trial")
        .setting("s3.client.snapshot_based_recoveries.protocol", () -> "http", (n) -> USE_FIXTURE)
        .setting("s3.client.snapshot_based_recoveries.endpoint", s3Fixture::getAddress, (n) -> USE_FIXTURE)
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
    protected Settings repositorySettings() {
        final String bucket = System.getProperty("test.s3.bucket");
        assertThat(bucket, not(blankOrNullString()));

        final String basePath = System.getProperty("test.s3.base_path");
        assertThat(basePath, not(blankOrNullString()));

        return Settings.builder().put("client", "snapshot_based_recoveries").put("bucket", bucket).put("base_path", basePath).build();
    }
}

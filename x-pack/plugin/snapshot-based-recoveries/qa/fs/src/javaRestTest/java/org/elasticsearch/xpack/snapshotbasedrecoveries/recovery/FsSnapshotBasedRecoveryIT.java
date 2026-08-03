/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.snapshotbasedrecoveries.recovery;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;

public class FsSnapshotBasedRecoveryIT extends AbstractSnapshotBasedRecoveryRestTestCase {

    private static final TemporaryFolder repoDirectory = new TemporaryFolder();

    private static final ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .nodes(3)
        .setting("xpack.license.self_generated.type", "trial")
        .setting("path.repo", () -> repoDirectory.getRoot().getPath())
        .setting("xpack.security.enabled", "false")
        .build();

    @ClassRule
    public static TestRule ruleChain = RuleChain.outerRule(repoDirectory).around(cluster);

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected String repositoryType() {
        return "fs";
    }

    @Override
    protected Settings repositorySettings() {
        return Settings.builder().put("location", repoDirectory.getRoot().getPath()).build();
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.repositories.blobstore.testkit.analyze;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;

public class FsRepositoryAnalysisRestIT extends AbstractRepositoryAnalysisRestTestCase {

    private static final TemporaryFolder repoDirectory = new TemporaryFolder();

    private static final ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .module("snapshot-repo-test-kit")
        .setting("path.repo", FsRepositoryAnalysisRestIT::repoDirectoryPath)
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
        return FsRepository.TYPE;
    }

    @Override
    protected Settings repositorySettings() {
        return Settings.builder().put("location", repoDirectoryPath()).build();
    }

    @SuppressForbidden(reason = "TemporaryFolder uses java.io.File")
    private static String repoDirectoryPath() {
        return repoDirectory.getRoot().getPath();
    }
}

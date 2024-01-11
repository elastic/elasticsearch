/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchablesnapshots;

import fixture.url.URLFixture;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import static org.hamcrest.Matchers.blankOrNullString;
import static org.hamcrest.Matchers.not;

public class URLSearchableSnapshotsIT extends AbstractSearchableSnapshotsRestTestCase {

    public static URLFixture urlFixture = new URLFixture();

    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .setting("xpack.license.self_generated.type", "trial")
        .setting("repositories.url.allowed_urls", () -> urlFixture.getAddress())
        .setting("path.repo", () -> urlFixture.getRepositoryDir())
        .setting("xpack.searchable.snapshot.shared_cache.size", "16MB")
        .setting("xpack.searchable.snapshot.shared_cache.region_size", "256KB")
        .setting("xpack.searchable_snapshots.cache_fetch_async_thread_pool.keep_alive", "0ms")
        .setting("xpack.security.enabled", "false")
        .build();

    @ClassRule
    public static TestRule ruleChain = RuleChain.outerRule(urlFixture).around(cluster);

    @Override
    protected String writeRepositoryType() {
        return FsRepository.TYPE;
    }

    @Override
    protected Settings writeRepositorySettings() {
        final String repoDirectory = urlFixture.getRepositoryDir();
        assertThat(repoDirectory, not(blankOrNullString()));

        return Settings.builder().put("location", repoDirectory).build();
    }

    @Override
    protected boolean useReadRepository() {
        return true;
    }

    @Override
    protected String readRepositoryType() {
        return "url";
    }

    @Override
    protected Settings readRepositorySettings() {
        final String url = urlFixture.getAddress();
        assertThat(url, not(blankOrNullString()));

        return Settings.builder().put("url", url).build();
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }
}

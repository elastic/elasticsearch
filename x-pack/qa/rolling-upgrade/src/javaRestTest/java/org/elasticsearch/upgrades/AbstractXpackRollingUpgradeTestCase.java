/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.upgrades;

import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.cluster.util.Version;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;

import java.util.function.Supplier;

public abstract class AbstractXpackRollingUpgradeTestCase extends ParameterizedRollingUpgradeTestCase {

    private static final TemporaryFolder repoDirectory = new TemporaryFolder();

    private static final ElasticsearchCluster cluster = buildCluster();

    @ClassRule
    public static TestRule ruleChain = RuleChain.outerRule(repoDirectory).around(cluster);

    private static ElasticsearchCluster buildCluster() {
        var builder = ElasticsearchCluster.local()
            .distribution(DistributionType.DEFAULT)
            .version(getOldClusterVersion())
            .nodes(NODE_NUM)
            .setting("xpack.license.self_generated.type", "trial")
            .setting("xpack.security.enabled", "false")
            .systemProperty("ingest.geoip.downloader.enabled.default", "true")
            .systemProperty("ingest.geoip.downloader.endpoint.default", "http://invalid.endpoint")
            .setting("ingest.geoip.downloader.endpoint", "http://invalid.endpoint")
            .setting("path.repo", new Supplier<>() {
                @Override
                @SuppressForbidden(reason = "TemporaryFolder only has io.File methods, not nio.File")
                public String get() {
                    return repoDirectory.getRoot().getPath();
                }
            })
            .setting("xpack.searchable.snapshot.shared_cache.size", "16MB")
            .setting("xpack.searchable.snapshot.shared_cache.region_size", "256KB");

        if (getOldClusterTestVersion().before(Version.fromString("8.18.0"))) {
            builder.jvmArg("-da:org.elasticsearch.index.mapper.DocumentMapper");
            builder.jvmArg("-da:org.elasticsearch.index.mapper.MapperService");
        }

        return builder.build();
    }

    public AbstractXpackRollingUpgradeTestCase(int upgradedNodes) {
        super(upgradedNodes);
    }

    @Override
    protected ElasticsearchCluster getUpgradeCluster() {
        return cluster;
    }
}

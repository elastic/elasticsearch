/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.shutdown;

import com.carrotsearch.randomizedtesting.annotations.Name;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.upgrades.ParameterizedRollingUpgradeTestCase;
import org.junit.ClassRule;

import java.io.IOException;

/**
 * Base class for the node shutdown rolling upgrade tests. Keeps the shared cluster and
 * security/auth wiring that {@code AbstractUpgradeTestCase} used to provide, adapted to the
 * {@link ParameterizedRollingUpgradeTestCase} JUnit rule based upgrade model.
 */
public abstract class NodeShutdownRollingUpgradeTestCase extends ParameterizedRollingUpgradeTestCase {

    private static final String USER = "test_user";
    private static final String PASS = "x-pack-test-password";

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .version(getOldClusterVersion(), isOldClusterDetachedVersion())
        .nodes(NODE_NUM)
        .setting("xpack.security.enabled", "true")
        .setting("xpack.license.self_generated.type", "trial")
        .user(USER, PASS)
        .build();

    protected NodeShutdownRollingUpgradeTestCase(@Name("upgradedNodes") int upgradedNodes) {
        super(upgradedNodes);
    }

    @Override
    protected ElasticsearchCluster getUpgradeCluster() {
        return cluster;
    }

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue(USER, new SecureString(PASS.toCharArray()));
        return Settings.builder().put(super.restClientSettings()).put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    @Override
    protected void deleteAllNodeShutdownMetadata() throws IOException {
        // do not delete node shutdown records; the test relies on shutdown records created in
        // earlier parameterized upgrade stages surviving into later ones.
    }
}

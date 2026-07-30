/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.upgrades;

import com.carrotsearch.randomizedtesting.annotations.TestCaseOrdering;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.test.AnnotationTestOrdering;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.FeatureFlag;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.cluster.util.Version;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.function.Supplier;

/**
 * At a high-level, this testing infrastructure will allow us to write tests that:
 * <p>
 * Boot  nodes on V_7
 * <p>
 * Add a lifecycle hook for individual tests to create their own system indices.
 * <p>
 * Upgrade the nodes to V_8
 * <p>
 * Perform a POST request to the feature update  API
 * <p>
 * Poll the feature update API till it's ready
 * <p>
 * Add a lifecycle hook for the individual tests to use their system index, and ensure it works.
 * <p>
 * Upgrade the nodes to V_9, repeat steps 4-6
 */
@TestCaseOrdering(AnnotationTestOrdering.class)
public abstract class AbstractSystemIndicesTestCase extends ESRestTestCase {

    abstract void onOldCluster();

    abstract void afterFirstUpgrade();

    abstract void afterSecondUpgrade();

    @AnnotationTestOrdering.Order(0)
    public final void testSystemIndicesUpgrade() throws IOException {
        onOldCluster();
        upgradeCluster(getSecondClusterVersion());
        afterFirstUpgrade();
        upgradeCluster(getFinalClusterVersion());
        afterSecondUpgrade();
    }

    private static final TemporaryFolder repoDirectory = new TemporaryFolder();

    public ElasticsearchCluster getCluster() {
        return ElasticsearchCluster.local()
            .distribution(DistributionType.DEFAULT)
            .version(getFirstClusterVersion())
            .nodes(getNumberOfNodes())
            .setting("path.repo", new Supplier<>() {
                @Override
                @SuppressForbidden(reason = "TemporaryFolder only has io.File methods, not nio.File")
                public String get() {
                    return repoDirectory.getRoot().getPath();
                }
            })
            .setting("xpack.security.enabled", "false")
            .feature(FeatureFlag.TIME_SERIES_MODE)
            .build();
    }

    public int getNumberOfNodes() {
        return 3;
    }

    public Version getFirstClusterVersion() {
        return Version.fromString("v7.17.26"); // todo
    }

    public Version getSecondClusterVersion() {
        return Version.fromString("v8.16.1"); // todo
    }

    public Version getFinalClusterVersion() {
        return Version.CURRENT;
    }

    protected void upgradeCluster(Version version) throws IOException {
        closeClients();
        for (int n = 0; n < getNumberOfNodes(); n++) {
            try {
                logger.info("Upgrading node {} to version {}", n, version);
                getCluster().upgradeNodeToVersion(n, version);
            } catch (Exception e) {
                logger.error("Failure upgrading node {} to version {}", n, version, e);
                throw e;
            }
        }
        initClient();
    }

}

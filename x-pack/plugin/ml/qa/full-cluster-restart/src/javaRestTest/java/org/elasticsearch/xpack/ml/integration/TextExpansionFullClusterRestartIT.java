/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.integration;

import com.carrotsearch.randomizedtesting.annotations.Name;

import org.elasticsearch.Version;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.upgrades.FullClusterRestartUpgradeStatus;
import org.elasticsearch.upgrades.ParameterizedFullClusterRestartTestCase;
import org.junit.ClassRule;

import static org.elasticsearch.Version.V_8_13_0;

public class TextExpansionFullClusterRestartIT extends ParameterizedFullClusterRestartTestCase {

    private static final Version TOKEN_PRUNING_ADDED_VERSION = V_8_13_0;

    @ClassRule
    public static final ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .version(getOldClusterTestVersion())
        .nodes(2)
        .setting("xpack.security.enabled", "false")
        .module("x-pack-ml")
        .build();

    public TextExpansionFullClusterRestartIT(@Name("cluster") FullClusterRestartUpgradeStatus upgradeStatus) {
        super(upgradeStatus);
    }

    @Override
    protected ElasticsearchCluster getUpgradeCluster() {
        return cluster;
    }

    public void testTextExpansionSearch() throws Exception {
        assumeTrue(
            "Data retention changed by default to DSL in " + TOKEN_PRUNING_ADDED_VERSION,
            getOldClusterTestVersion().before(TOKEN_PRUNING_ADDED_VERSION.toString())
        );

        if (isRunningAgainstOldCluster()) {
            // TODO
        } else {
            // TODO
        }
    }
}

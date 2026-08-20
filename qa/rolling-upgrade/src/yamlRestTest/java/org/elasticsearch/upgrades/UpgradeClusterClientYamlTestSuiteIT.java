/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.upgrades;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.TimeoutSuite;

import org.apache.lucene.tests.util.TimeUnits;
import org.elasticsearch.test.RollingUpgradePerformer;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.elasticsearch.test.rest.yaml.ParameterizedYamlRollingUpgradeTestCase;
import org.junit.ClassRule;

@TimeoutSuite(millis = 5 * TimeUnits.MINUTE) // to account for slow as hell VMs
public class UpgradeClusterClientYamlTestSuiteIT extends ParameterizedYamlRollingUpgradeTestCase {

    @ClassRule
    public static final ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .version(RollingUpgradePerformer.getOldClusterVersion(), RollingUpgradePerformer.isOldClusterDetachedVersion())
        .nodes(NODE_NUM)
        .setting("repositories.url.allowed_urls", "http://snapshot.test*")
        .setting("xpack.security.enabled", "false")
        .build();

    public UpgradeClusterClientYamlTestSuiteIT(@Name("upgradedNodes") int upgradedNodes, ClientYamlTestCandidate testCandidate) {
        super(upgradedNodes, testCandidate);
    }

    @Override
    protected ElasticsearchCluster getUpgradeCluster() {
        return cluster;
    }
}

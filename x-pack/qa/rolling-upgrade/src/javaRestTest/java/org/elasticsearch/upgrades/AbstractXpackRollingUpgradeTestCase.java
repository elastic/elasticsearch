/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.upgrades;

import org.elasticsearch.Build;
import org.elasticsearch.test.ParameterizedRollingUpgradeTestCase;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.LocalClusterSpecBuilder;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.cluster.util.Version;

import java.util.function.UnaryOperator;

public abstract class AbstractXpackRollingUpgradeTestCase extends ParameterizedRollingUpgradeTestCase {

    static final org.elasticsearch.Version UPGRADE_FROM_VERSION = org.elasticsearch.Version.fromString(
        System.getProperty("tests.upgrade_from_version")
    );

    public AbstractXpackRollingUpgradeTestCase(int upgradedNodes) {
        super(upgradedNodes);
    }

    protected static ElasticsearchCluster buildCluster() {
        return buildCluster(b -> b);
    }

    protected static ElasticsearchCluster buildCluster(UnaryOperator<LocalClusterSpecBuilder<ElasticsearchCluster>> customizer) {
        var builder = ElasticsearchCluster.local()
            .distribution(DistributionType.DEFAULT)
            .version(getOldClusterVersion())
            .nodes(NODE_NUM)
            .setting("xpack.license.self_generated.type", "trial")
            .setting("xpack.security.enabled", "false");

        if (getOldClusterTestVersion().before(Version.fromString("8.18.0"))) {
            builder.jvmArg("-da:org.elasticsearch.index.mapper.DocumentMapper");
            builder.jvmArg("-da:org.elasticsearch.index.mapper.MapperService");
        }

        return customizer.apply(builder).build();
    }

    protected static boolean isOriginalClusterCurrent() {
        return getOldClusterVersion().equals(Build.current().version());
    }
}

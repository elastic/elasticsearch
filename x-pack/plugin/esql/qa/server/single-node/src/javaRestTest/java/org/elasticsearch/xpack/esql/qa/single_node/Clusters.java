/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.single_node;

import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.FeatureFlag;
import org.elasticsearch.test.cluster.local.LocalClusterConfigProvider;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.elasticsearch.xpack.esql.CsvTestUtils;

import java.nio.file.Path;

public class Clusters {

    public static ElasticsearchCluster testCluster() {
        return testCluster(config -> {});
    }

    public static ElasticsearchCluster testCluster(LocalClusterConfigProvider configProvider) {
        return testCluster(CsvTestUtils.createCsvDataDirectory(), configProvider);
    }

    public static ElasticsearchCluster testCluster(Path csvDataPath, LocalClusterConfigProvider configProvider) {
        return ElasticsearchCluster.local()
            .distribution(DistributionType.DEFAULT)
            .setting("xpack.security.enabled", "false")
            .setting("xpack.license.self_generated.type", "trial")
            .setting("path.repo", csvDataPath::toString)
            .shared(true)
            .configFile("user-agent/custom-regexes.yml", Resource.fromClasspath("custom-regexes.yml"))
            .apply(() -> configProvider)
            .feature(FeatureFlag.EXTENDED_DOC_VALUES_PARAMS)
            .build();
    }
}

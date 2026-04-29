/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.single_node;

import org.elasticsearch.core.Booleans;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.FeatureFlag;
import org.elasticsearch.test.cluster.local.LocalClusterConfigProvider;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.elasticsearch.xpack.esql.CsvTestUtils;

import java.nio.file.Path;

public class Clusters {

    /**
     * System property that, when set to {@code "true"}, enables xpack security on the test
     * cluster and provisions a single superuser. The corresponding credentials must also be
     * passed via {@code tests.rest.cluster.username} / {@code tests.rest.cluster.password} so
     * that {@link org.elasticsearch.test.rest.ESRestTestCase#restClientSettings()} picks them
     * up and authenticates the REST client. Defaults to {@code "false"} (security disabled).
     */
    public static final String SECURITY_ENABLED_PROPERTY = "tests.cluster.security.enabled";

    public static final String ADMIN_USER = System.getProperty("tests.rest.cluster.username", "test-admin");
    public static final String ADMIN_PASSWORD = System.getProperty("tests.rest.cluster.password", "x-pack-test-password");

    public static ElasticsearchCluster testCluster() {
        return testCluster(config -> {});
    }

    public static ElasticsearchCluster testCluster(LocalClusterConfigProvider configProvider) {
        return testCluster(CsvTestUtils.createCsvDataDirectory(), configProvider);
    }

    public static ElasticsearchCluster testCluster(Path csvDataPath, LocalClusterConfigProvider configProvider) {
        boolean securityEnabled = Booleans.parseBoolean(System.getProperty(SECURITY_ENABLED_PROPERTY, "false"));
        var builder = ElasticsearchCluster.local()
            .distribution(DistributionType.DEFAULT)
            .setting("xpack.security.enabled", Boolean.toString(securityEnabled))
            .setting("xpack.license.self_generated.type", "trial")
            .setting("path.repo", csvDataPath::toString)
            .shared(true)
            .configFile("user-agent/custom-regexes.yml", Resource.fromClasspath("custom-regexes.yml"))
            .apply(() -> configProvider)
            .feature(FeatureFlag.EXTENDED_DOC_VALUES_PARAMS);
        if (securityEnabled) {
            builder.user(ADMIN_USER, ADMIN_PASSWORD, "superuser", true);
        }
        return builder.build();
    }
}

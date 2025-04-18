/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.datastreams;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.LocalClusterSpecBuilder;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.elasticsearch.test.rest.yaml.ESClientYamlSuiteTestCase;
import org.junit.ClassRule;

public class DataStreamsClientYamlTestSuiteIT extends ESClientYamlSuiteTestCase {

    public DataStreamsClientYamlTestSuiteIT(final ClientYamlTestCandidate testCandidate) {
        super(testCandidate);
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws Exception {
        return createParameters();
    }

    private static final String BASIC_AUTH_VALUE = basicAuthHeaderValue("x_pack_rest_user", new SecureString("x-pack-test-password"));

    @Override
    protected Settings restClientSettings() {
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", BASIC_AUTH_VALUE).build();
    }

    @ClassRule
    public static ElasticsearchCluster cluster = createCluster();

    private static ElasticsearchCluster createCluster() {
        LocalClusterSpecBuilder<ElasticsearchCluster> clusterBuilder = ElasticsearchCluster.local()
            .distribution(DistributionType.DEFAULT)
            .setting("xpack.security.enabled", "true")
            .keystore("bootstrap.password", "x-pack-test-password")
            .user("x_pack_rest_user", "x-pack-test-password")
            .systemProperty("es.queryable_built_in_roles_enabled", "false");
        if (initTestSeed().nextBoolean()) {
            clusterBuilder.setting("xpack.license.self_generated.type", "trial");
        }
        boolean setNodes = Boolean.parseBoolean(System.getProperty("yaml.rest.tests.set_num_nodes", "true"));
        if (setNodes) {
            clusterBuilder.nodes(2);
        }
        return clusterBuilder.build();
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

}

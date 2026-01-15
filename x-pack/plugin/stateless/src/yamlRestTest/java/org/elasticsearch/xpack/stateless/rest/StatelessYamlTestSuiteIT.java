/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.rest;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.LocalClusterSpecBuilder;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.elasticsearch.test.rest.yaml.ESClientYamlSuiteTestCase;
import org.junit.ClassRule;

public class StatelessYamlTestSuiteIT extends ESClientYamlSuiteTestCase {

    public StatelessYamlTestSuiteIT(final ClientYamlTestCandidate testCandidate) {
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
            .user("x_pack_rest_user", "x-pack-test-password");
        boolean setNodes = Booleans.parseBoolean(System.getProperty("yaml.rest.tests.set_num_nodes", "true"));
        if (setNodes) {
            clusterBuilder.nodes(2);
        }
        // We need to disable ILM history based on a setting, to avoid errors in Serverless where the setting is not available.
        boolean disableILMHistory = Booleans.parseBoolean(System.getProperty("yaml.rest.tests.disable_ilm_history", "true"));
        if (disableILMHistory) {
            // disable ILM history, since it disturbs tests
            clusterBuilder.setting("indices.lifecycle.history_index_enabled", "false");
        }
        return clusterBuilder.build();
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

}

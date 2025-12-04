/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.Build;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.FeatureFlag;
import org.elasticsearch.test.cluster.local.LocalClusterSpecBuilder;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.elasticsearch.test.rest.yaml.ESClientYamlSuiteTestCase;
import org.junit.ClassRule;

import java.util.ArrayList;
import java.util.List;

public class LogsdbTestSuiteIT extends ESClientYamlSuiteTestCase {

    private static final String USER = "x_pack_rest_user";
    private static final String PASS = "x-pack-test-password";

    @ClassRule
    public static final ElasticsearchCluster cluster = createCluster();

    private static ElasticsearchCluster createCluster() {
        LocalClusterSpecBuilder<ElasticsearchCluster> clusterBuilder = ElasticsearchCluster.local()
            .distribution(DistributionType.DEFAULT)
            .setting("xpack.security.enabled", "true")
            .user(USER, PASS)
            .keystore("bootstrap.password", "x-pack-test-password")
            .setting("xpack.license.self_generated.type", "trial")
            .feature(FeatureFlag.DOC_VALUES_SKIPPER);
        boolean setNodes = Booleans.parseBoolean(System.getProperty("yaml.rest.tests.set_num_nodes", "true"));
        if (setNodes) {
            clusterBuilder.nodes(1);
        }
        return clusterBuilder.build();
    }

    public LogsdbTestSuiteIT(@Name("yaml") ClientYamlTestCandidate testCandidate) {
        super(testCandidate);
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws Exception {
        // Filter out 52_esql_insist_operator_synthetic_source.yml suite for snapshot builds:
        // (esql doesn't use feature flags and all experimental features are just enabled if build is snapshot)

        List<Object[]> filtered = new ArrayList<>();
        for (Object[] params : ESClientYamlSuiteTestCase.createParameters()) {
            ClientYamlTestCandidate candidate = (ClientYamlTestCandidate) params[0];
            if (candidate.getRestTestSuite().getName().equals("52_esql_insist_operator_synthetic_source")
                && Build.current().isSnapshot() == false) {
                continue;
            }
            filtered.add(new Object[] { candidate });
        }
        return filtered;
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue(USER, new SecureString(PASS.toCharArray()));
        return Settings.builder().put(super.restClientSettings()).put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

}

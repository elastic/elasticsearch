/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.smoketest;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.elasticsearch.test.rest.yaml.ESClientYamlSuiteTestCase;
import org.junit.ClassRule;

import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;

public class GraphWithSecurityIT extends ESClientYamlSuiteTestCase {

    private static final String TEST_ADMIN_USERNAME = "test_admin";
    private static final String TEST_ADMIN_PASSWORD = "x-pack-test-password";

    @ClassRule
    public static ElasticsearchCluster cluster = createCluster();

    public static ElasticsearchCluster createCluster() {
        return ElasticsearchCluster.local()
            .distribution(DistributionType.DEFAULT)
            .setting("xpack.security.enabled", "true")
            .setting("xpack.license.self_generated.type", "trial")
            .rolesFile(Resource.fromClasspath("roles.yml"))
            .user(TEST_ADMIN_USERNAME, TEST_ADMIN_PASSWORD, "superuser", false)
            .user("graph_explorer", "x-pack-test-password", "graph_explorer", false)
            .user("no_graph_explorer", "x-pack-test-password", "no_graph_explorer", false)
            .build();
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    public GraphWithSecurityIT(@Name("yaml") ClientYamlTestCandidate testCandidate) {
        super(testCandidate);
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws Exception {
        return createParameters();
    }

    protected String[] getCredentials() {
        return new String[] { "graph_explorer", "x-pack-test-password" };
    }

    @Override
    protected Settings restClientSettings() {
        String[] creds = getCredentials();
        String token = basicAuthHeaderValue(creds[0], new SecureString(creds[1].toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    @Override
    protected Settings restAdminSettings() {
        String token = basicAuthHeaderValue(TEST_ADMIN_USERNAME, new SecureString(TEST_ADMIN_PASSWORD.toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }
}

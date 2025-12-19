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
import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.elasticsearch.test.rest.yaml.ESClientYamlSuiteTestCase;
import org.junit.ClassRule;

public class SmokeTestSecurityWithMustacheClientYamlTestSuiteIT extends ESClientYamlSuiteTestCase {

    private static final String BASIC_AUTH_VALUE = basicAuthHeaderValue(
        "test_admin",
        new SecureString("x-pack-test-password".toCharArray())
    );

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .setting("xpack.watcher.enabled", "false")
        .setting("xpack.security.enabled", "true")
        .setting("xpack.license.self_generated.type", "trial")
        .user("test_admin", "x-pack-test-password")
        .build();

    public SmokeTestSecurityWithMustacheClientYamlTestSuiteIT(@Name("yaml") ClientYamlTestCandidate testCandidate) {
        super(testCandidate);
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws Exception {
        return ESClientYamlSuiteTestCase.createParameters();
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected Settings restClientSettings() {
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", BASIC_AUTH_VALUE).build();
    }
}

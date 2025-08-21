/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.elasticsearch.test.rest.yaml.ESClientYamlSuiteTestCase;
import org.junit.ClassRule;

public class DownsampleWithSecurityRestIT extends ESClientYamlSuiteTestCase {

    public static final String USERNAME = "elastic_admin";
    public static final String PASSWORD = "admin-password";

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .setting("xpack.license.self_generated.type", "trial")
        .setting("xpack.security.enabled", "true")
        .user(USERNAME, PASSWORD)
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    public DownsampleWithSecurityRestIT(final ClientYamlTestCandidate testCandidate) {
        super(testCandidate);
    }

    @Override
    protected Settings restClientSettings() {
        String authentication = basicAuthHeaderValue(USERNAME, new SecureString(PASSWORD.toCharArray()));
        return Settings.builder().put(super.restClientSettings()).put(ThreadContext.PREFIX + ".Authorization", authentication).build();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws Exception {
        return ESClientYamlSuiteTestCase.createParameters(new String[] { "downsample-with-security" });
    }
}

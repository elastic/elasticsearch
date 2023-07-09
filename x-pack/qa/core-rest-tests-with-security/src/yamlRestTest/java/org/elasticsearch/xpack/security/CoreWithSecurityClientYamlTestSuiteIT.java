/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import com.carrotsearch.randomizedtesting.annotations.TimeoutSuite;

import org.apache.lucene.tests.util.TimeUnits;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.FeatureFlag;
import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.elasticsearch.test.rest.yaml.ESClientYamlSuiteTestCase;
import org.junit.ClassRule;

import java.util.Objects;

@TimeoutSuite(millis = 30 * TimeUnits.MINUTE) // as default timeout seems not enough on the jenkins VMs
public class CoreWithSecurityClientYamlTestSuiteIT extends ESClientYamlSuiteTestCase {
    private static final String USER = Objects.requireNonNull(System.getProperty("tests.rest.cluster.username", "test_admin"));
    private static final String PASS = Objects.requireNonNull(System.getProperty("tests.rest.cluster.password", "x-pack-test-password"));

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .module("constant-keyword")
        .module("mapper-extras")
        .module("rank-eval")
        .module("x-pack-ilm")
        .module("x-pack-stack")
        .module("ingest-common")
        .module("reindex")
        .module("wildcard")
        .setting("xpack.security.enabled", "true")
        .setting("xpack.watcher.enabled", "false")
        .setting("xpack.ml.enabled", "false")
        .setting("xpack.license.self_generated.type", "trial")
        .setting("xpack.security.autoconfiguration.enabled", "false")
        .user(USER, PASS)
        .feature(FeatureFlag.TIME_SERIES_MODE)
        .feature(FeatureFlag.DATA_STREAM_LIFECYCLE_ENABLED)
        .feature(FeatureFlag.SYNONYMS_ENABLED)
        .build();

    public CoreWithSecurityClientYamlTestSuiteIT(@Name("yaml") ClientYamlTestCandidate testCandidate) {
        super(testCandidate);
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws Exception {
        return ESClientYamlSuiteTestCase.createParameters();
    }

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue(USER, new SecureString(PASS.toCharArray()));
        return Settings.builder().put(super.restClientSettings()).put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }
}

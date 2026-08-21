/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.oteldata;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.common.util.FeatureFlag;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.elasticsearch.test.rest.yaml.ESClientYamlSuiteTestCase;
import org.junit.Before;
import org.junit.ClassRule;

public class OTelYamlTestSuiteIT extends ESClientYamlSuiteTestCase {

    private static final FeatureFlag METRIC_EXEMPLARS_FEATURE_FLAG = new FeatureFlag("metric_exemplars");

    private final boolean exemplarTest;

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .module("constant-keyword")
        .module("counted-keyword")
        .module("data-streams")
        .module("ingest-common")
        .module("ip-location")
        .module("ingest-ip-location")
        .module("user-agent")
        .module("lang-mustache")
        .module("mapper-extras")
        .module("wildcard")
        .module("x-pack-analytics")
        .module("x-pack-otel-data")
        .module("x-pack-aggregate-metric")
        .module("x-pack-ilm")
        .module("x-pack-stack")
        .module("mapper-version")
        .setting("ingest.geoip.downloader.enabled", "false")
        .build();

    public OTelYamlTestSuiteIT(@Name("yaml") ClientYamlTestCandidate testCandidate) {
        super(testCandidate);
        String testPath = testCandidate.getTestPath();
        exemplarTest = testPath.contains("/20_exemplars_tests/")
            || testPath.endsWith("/10_otel/Test exemplars-otel* template installation");
    }

    @Before
    public void skipExemplarTestsWhenFeatureFlagIsDisabled() {
        assumeTrue("requires the metric_exemplars feature flag", exemplarTest == false || METRIC_EXEMPLARS_FEATURE_FLAG.isEnabled());
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws Exception {
        return ESClientYamlSuiteTestCase.createParameters();
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }
}

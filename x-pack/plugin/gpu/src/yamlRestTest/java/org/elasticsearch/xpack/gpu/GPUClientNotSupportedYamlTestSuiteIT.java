/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.gpu;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.elasticsearch.test.rest.yaml.ESClientYamlSuiteTestCase;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

/**
 * Runs YAML REST tests under {@code gpu/not-supported} only on machines
 * where GPU hardware is NOT available. Uses {@link GPUNotSupportedRule}
 * to skip when a GPU is detected.
 */
public class GPUClientNotSupportedYamlTestSuiteIT extends ESClientYamlSuiteTestCase {

    public static ElasticsearchCluster cluster = createCluster();

    public static GPUNotSupportedRule gpuNotSupportedRule = new GPUNotSupportedRule();

    private static ElasticsearchCluster createCluster() {
        var builder = ElasticsearchCluster.local()
            .distribution(DistributionType.DEFAULT)
            .nodes(1)
            .module("gpu")
            .setting("xpack.license.self_generated.type", "trial")
            .setting("xpack.security.enabled", "false")
            .setting("vectors.indexing.use_gpu", "false");

        return builder.build();
    }

    @ClassRule
    public static TestRule ruleChain = RuleChain.outerRule(gpuNotSupportedRule).around(cluster);

    public GPUClientNotSupportedYamlTestSuiteIT(final ClientYamlTestCandidate testCandidate) {
        super(testCandidate);
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws Exception {
        return ESClientYamlSuiteTestCase.createParameters("gpu/not-supported");
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }
}

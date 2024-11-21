/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.elasticsearch.test.rest.yaml.ESClientYamlSuiteTestCase;
import org.junit.ClassRule;

public class InferenceRestIT extends ESClientYamlSuiteTestCase {

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .setting("xpack.security.enabled", "false")
        .setting("xpack.security.http.ssl.enabled", "false")
        .setting("xpack.license.self_generated.type", "trial")
        .plugin("inference-service-test")
        .distribution(DistributionType.DEFAULT)
        .build();

    public InferenceRestIT(final ClientYamlTestCandidate testCandidate) {
        super(testCandidate);
    }

    @Override
    protected Settings restClientSettings() {
        var baseSettings = super.restClientSettings();
        return Settings.builder()
            .put(baseSettings)
            .put(CLIENT_SOCKET_TIMEOUT, "300s")  // Long timeout for model download
            .build();
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws Exception {
        return ESClientYamlSuiteTestCase.createParameters();
    }
}

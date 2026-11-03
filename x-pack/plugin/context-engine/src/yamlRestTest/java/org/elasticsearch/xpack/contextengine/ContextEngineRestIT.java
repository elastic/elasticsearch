/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.contextengine;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.elasticsearch.test.rest.yaml.ESClientYamlSuiteTestCase;
import org.junit.ClassRule;

public class ContextEngineRestIT extends ESClientYamlSuiteTestCase {

    private static final String USER = "x_pack_rest_user";
    private static final String PASSWORD = "x-pack-test-password";

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .module("x-pack-context-engine")
        .module("data-streams")
        .module("x-pack-inference")
        .module("rank-rrf")
        .plugin("inference-service-test")
        .setting("xpack.context_engine.session_memory.inference_id", "test-embedding")
        .setting("xpack.context_engine.session_memory.rerank_inference_id", "test-rerank")
        .setting("xpack.security.enabled", "true")
        .setting("xpack.license.self_generated.type", "trial")
        .user(USER, PASSWORD)
        .build();

    public ContextEngineRestIT(@Name("yaml") final ClientYamlTestCandidate testCandidate) {
        super(testCandidate);
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws Exception {
        return createParameters();
    }

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue(USER, new SecureString(PASSWORD.toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }
}

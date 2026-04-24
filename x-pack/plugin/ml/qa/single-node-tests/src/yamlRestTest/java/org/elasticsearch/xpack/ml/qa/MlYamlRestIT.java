/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.qa;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.http.HttpStatus;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.test.SecuritySettingsSourceField;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.elasticsearch.test.rest.yaml.ClientYamlTestResponse;
import org.elasticsearch.test.rest.yaml.ClientYamlTestResponseException;
import org.elasticsearch.test.rest.yaml.ESClientYamlSuiteTestCase;
import org.elasticsearch.xpack.core.ml.integration.MlRestTestStateCleaner;
import org.elasticsearch.xpack.inference.MlModelServer;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * Runs the ML YAML REST test suite against a dedicated single-node cluster.
 *
 * <p>Previously these tests lived in the monolithic {@code :x-pack:plugin:yamlRestTest}
 * suite alongside every other x-pack feature. They were relocated here so that ML
 * can own its cluster configuration, state-cleanup helpers and {@link MlModelServer}
 * class rule independently of unrelated features. The YAML test sources still live
 * under {@code x-pack/plugin/src/yamlRestTest/resources/rest-api-spec/test/ml/} so
 * that internal consumers using {@code restResources.restTests.includeXpack 'ml'}
 * continue to work; this project pulls them in via the same mechanism.
 */
public class MlYamlRestIT extends ESClientYamlSuiteTestCase {

    private static final String BASIC_AUTH_VALUE = basicAuthHeaderValue(
        "x_pack_rest_user",
        SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING
    );

    private static final MlModelServer ML_MODEL_SERVER = new MlModelServer();

    private static final ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .name("ml-yamlRestTest")
        .setting("xpack.ml.enabled", "true")
        .setting("xpack.security.enabled", "true")
        .setting("xpack.watcher.enabled", "false")
        .setting("xpack.security.authc.token.enabled", "true")
        .setting("xpack.security.authc.api_key.enabled", "true")
        .setting("xpack.license.self_generated.type", "trial")
        // Disable ILM history because several ML tests use _all index patterns.
        .setting("indices.lifecycle.history_index_enabled", "false")
        .keystore("bootstrap.password", "x-pack-test-password")
        .user("x_pack_rest_user", "x-pack-test-password")
        .build();

    // Apply MlModelServer first so it is listening before the cluster boots.
    @ClassRule
    public static final TestRule ruleChain = RuleChain.outerRule(ML_MODEL_SERVER).around(cluster);

    public MlYamlRestIT(ClientYamlTestCandidate testCandidate) {
        super(testCandidate);
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws Exception {
        return createParameters();
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected Settings restClientSettings() {
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", BASIC_AUTH_VALUE).build();
    }

    /**
     * Point the cluster at our in-process {@link MlModelServer} so that tests which
     * install 3rd-party trained models (e.g. {@code .elser_model_2}) can fetch them
     * without hitting the public internet.
     */
    @Before
    public void setMlModelRepository() throws IOException {
        assertOK(ML_MODEL_SERVER.setMlModelRepository(adminClient()));
    }

    /**
     * Wait for the trial license to be installed; some tests rely on it being active
     * before their first request.
     */
    @Before
    public void waitForLicense() {
        awaitCallApi(
            "license.get",
            Map.of(),
            List.of(),
            response -> true,
            () -> "Exception when waiting for initial license to be generated",
            30
        );
    }

    /**
     * Reset ML state between tests (delete pipelines with inference processors and
     * invoke {@code _features/_reset}) and drain any cluster-state updates it triggers
     * before the base-class pending-tasks check runs.
     */
    @After
    public void cleanup() throws Exception {
        new MlRestTestStateCleaner(logger, adminClient()).resetFeatures();
        // _features/_reset can enqueue async cluster-state updates (e.g. inference
        // endpoint cache invalidation via ClearInferenceEndpointCacheAction). Drain
        // them here so the subsequent waitForPendingTasks does not race with them.
        waitForClusterUpdates();
    }

    private void awaitCallApi(
        String apiName,
        Map<String, String> params,
        List<Map<String, Object>> bodies,
        CheckedFunction<ClientYamlTestResponse, Boolean, IOException> success,
        Supplier<String> error,
        long maxWaitTimeInSeconds
    ) {
        try {
            final AtomicReference<ClientYamlTestResponse> response = new AtomicReference<>();
            assertBusy(() -> {
                try {
                    response.set(getAdminExecutionContext().callApi(apiName, params, bodies, Map.of()));
                    assertEquals(HttpStatus.SC_OK, response.get().getStatusCode());
                } catch (ClientYamlTestResponseException e) {
                    throw new AssertionError("Failed to call API " + apiName, e);
                }
            }, maxWaitTimeInSeconds, TimeUnit.SECONDS);
            success.apply(response.get());
        } catch (Exception e) {
            throw new IllegalStateException(error.get(), e);
        }
    }
}

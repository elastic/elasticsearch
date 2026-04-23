/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.qa.api;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.http.HttpStatus;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.test.SecuritySettingsSourceField;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.FeatureFlag;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.elasticsearch.test.rest.yaml.ClientYamlTestResponse;
import org.elasticsearch.test.rest.yaml.ClientYamlTestResponseException;
import org.elasticsearch.test.rest.yaml.ESClientYamlSuiteTestCase;
import org.junit.Before;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * Runs the security family of YAML rest tests (security, api_key, users, roles,
 * privileges, authenticate, change_password, role_mapping, service_accounts,
 * set_security_user, token, user_profile, ssl) previously hosted by the
 * monolithic {@code :x-pack:plugin:yamlRestTest} suite, against a
 * security-focused cluster without the ML/watcher/searchable-snapshots baggage
 * of {@code XPackRestIT}.
 */
public class SecurityApiYamlRestIT extends ESClientYamlSuiteTestCase {

    private static final String BASIC_AUTH_VALUE = basicAuthHeaderValue(
        "x_pack_rest_user",
        SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING
    );

    @ClassRule
    public static final ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .name("yamlRestTest")
        .setting("xpack.ml.enabled", "false")
        .setting("xpack.watcher.enabled", "false")
        .setting("xpack.security.enabled", "true")
        .setting("xpack.security.authc.token.enabled", "true")
        .setting("xpack.security.authc.api_key.enabled", "true")
        .setting("xpack.security.transport.ssl.enabled", "true")
        .setting("xpack.security.transport.ssl.key", "testnode.pem")
        .setting("xpack.security.transport.ssl.certificate", "testnode.crt")
        .setting("xpack.security.transport.ssl.verification_mode", "certificate")
        .setting("xpack.security.audit.enabled", "true")
        .setting("xpack.license.self_generated.type", "trial")
        // disable ILM history, since it disturbs tests using _all
        .setting("indices.lifecycle.history_index_enabled", "false")
        .keystore("bootstrap.password", "x-pack-test-password")
        .keystore("xpack.security.transport.ssl.secure_key_passphrase", "testnode")
        .user("x_pack_rest_user", "x-pack-test-password")
        .feature(FeatureFlag.TIME_SERIES_MODE)
        .feature(FeatureFlag.EXTENDED_DOC_VALUES_PARAMS)
        .configFile("testnode.pem", Resource.fromClasspath("org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.pem"))
        .configFile("testnode.crt", Resource.fromClasspath("org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.crt"))
        .configFile("service_tokens", Resource.fromClasspath("service_tokens"))
        .build();

    public SecurityApiYamlRestIT(ClientYamlTestCandidate testCandidate) {
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
     * Some security rest tests depend on the self-generated trial license being installed before they run.
     */
    @Before
    public void waitForLicense() {
        awaitCallApi(
            "license.get",
            Map.of(),
            List.of(),
            response -> true,
            () -> "Exception when waiting for initial license to be generated"
        );
    }

    private void awaitCallApi(
        String apiName,
        Map<String, String> params,
        List<Map<String, Object>> bodies,
        CheckedFunction<ClientYamlTestResponse, Boolean, IOException> success,
        Supplier<String> error
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
            }, 30, TimeUnit.SECONDS);
            success.apply(response.get());
        } catch (Exception e) {
            throw new IllegalStateException(error.get(), e);
        }
    }
}

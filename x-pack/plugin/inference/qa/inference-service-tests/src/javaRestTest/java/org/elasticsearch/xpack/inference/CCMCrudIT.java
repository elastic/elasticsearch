/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 *
 * this file has been contributed to by a Generative AI
 */

package org.elasticsearch.xpack.inference;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.action.CCMEnabledActionResponse;
import org.elasticsearch.xpack.core.inference.action.PutCCMConfigurationAction;
import org.junit.After;
import org.junit.ClassRule;

import java.io.IOException;

import static org.elasticsearch.xpack.core.inference.action.CCMEnabledActionResponse.ENABLED_FIELD_NAME;
import static org.elasticsearch.xpack.inference.InferenceBaseRestTest.assertStatusOkOrCreated;
import static org.elasticsearch.xpack.inference.rest.Paths.INFERENCE_CCM_PATH;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;

public class CCMCrudIT extends ESRestTestCase {

    static final String PUT_METHOD = "PUT";
    static final String GET_METHOD = "GET";

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .setting("xpack.license.self_generated.type", "basic")
        .setting("xpack.security.enabled", "true")
        .setting("xpack.inference.elastic.allow_configuring_ccm", "true")
        // This plugin is located in the inference/qa/test-service-plugin package, look for TestInferenceServicePlugin
        .plugin("inference-service-test")
        .user("x_pack_rest_user", "x-pack-test-password")
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue("x_pack_rest_user", new SecureString("x-pack-test-password".toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    @After
    public void cleanup() throws IOException {
        // Disable CCM after each test to ensure a clean state
        putCCMConfiguration(PutCCMConfigurationAction.Request.createDisabled(TimeValue.THIRTY_SECONDS, TimeValue.THIRTY_SECONDS));
    }

    public void testEnablesCCM_Succeeds() throws IOException {
        var response = putCCMConfiguration(
            PutCCMConfigurationAction.Request.createEnabled(
                new SecureString("key".toCharArray()),
                TimeValue.THIRTY_SECONDS,
                TimeValue.THIRTY_SECONDS
            )
        );

        assertTrue(response.isEnabled());
    }

    private static CCMEnabledActionResponse putCCMConfiguration(PutCCMConfigurationAction.Request request) throws IOException {
        var response = putRawRequest(INFERENCE_CCM_PATH, request);
        assertStatusOkOrCreated(response);
        var responseAsMap = entityAsMap(response);

        var enabled = responseAsMap.get(ENABLED_FIELD_NAME);

        assertThat(enabled, instanceOf(Boolean.class));
        return new CCMEnabledActionResponse((Boolean) enabled);
    }

    private static Response putRawRequest(String endpoint, PutCCMConfigurationAction.Request actionRequest) throws IOException {
        var builder = XContentFactory.contentBuilder(XContentType.JSON);
        actionRequest.toXContent(builder, null);

        var request = new Request(PUT_METHOD, endpoint);
        request.setJsonEntity(Strings.toString(builder));
        return client().performRequest(request);
    }

    public void testEnableCCM_WithNullApiKey_NullEnabled_ThrowsException() {
        var request = new PutCCMConfigurationAction.Request(null, null, TimeValue.THIRTY_SECONDS, TimeValue.THIRTY_SECONDS);

        var exception = expectThrows(IOException.class, () -> putCCMConfiguration(request));
        assertThat(exception.getMessage(), containsString("At least one of [api_key] or [enabled] must be provided"));
    }

    public void testEnableCCM_WithoutBody_ThrowsException() {
        var request = new Request(PUT_METHOD, INFERENCE_CCM_PATH);

        var exception = expectThrows(ResponseException.class, () -> client().performRequest(request));
        assertThat(exception.getMessage(), containsString("The body must be specified"));
    }

    public void testDisableCCM_WithEnabledTrue_ThrowsException() {
        var request = new PutCCMConfigurationAction.Request(null, true, TimeValue.THIRTY_SECONDS, TimeValue.THIRTY_SECONDS);

        var exception = expectThrows(IOException.class, () -> putCCMConfiguration(request));
        assertThat(exception.getMessage(), containsString("The [enabled] field must be set to [false] when disabling CCM"));
    }

    public void testDisableCCM_Succeeds() throws IOException {
        var response = putCCMConfiguration(
            PutCCMConfigurationAction.Request.createDisabled(TimeValue.THIRTY_SECONDS, TimeValue.THIRTY_SECONDS)
        );

        assertFalse(response.isEnabled());
    }

    public void testGetCCMConfiguration_WhenCCMDisabled_ReturnsDisabled() throws IOException {
        assertFalse(getCCMConfiguration().isEnabled());
    }

    private CCMEnabledActionResponse getCCMConfiguration() throws IOException {
        var getRequest = new Request(GET_METHOD, INFERENCE_CCM_PATH);
        var getResponse = client().performRequest(getRequest);
        assertStatusOkOrCreated(getResponse);
        var responseAsMap = entityAsMap(getResponse);

        var enabled = responseAsMap.get(ENABLED_FIELD_NAME);
        assertThat(enabled, instanceOf(Boolean.class));
        return new CCMEnabledActionResponse((Boolean) enabled);
    }

    public void testEnablesCCM_ThenDisable() throws IOException {
        var response = putCCMConfiguration(
            PutCCMConfigurationAction.Request.createEnabled(
                new SecureString("key".toCharArray()),
                TimeValue.THIRTY_SECONDS,
                TimeValue.THIRTY_SECONDS
            )
        );

        assertTrue(response.isEnabled());
        assertTrue(getCCMConfiguration().isEnabled());

        response = putCCMConfiguration(
            PutCCMConfigurationAction.Request.createDisabled(TimeValue.THIRTY_SECONDS, TimeValue.THIRTY_SECONDS)
        );

        assertFalse(response.isEnabled());
        assertFalse(getCCMConfiguration().isEnabled());
    }
}

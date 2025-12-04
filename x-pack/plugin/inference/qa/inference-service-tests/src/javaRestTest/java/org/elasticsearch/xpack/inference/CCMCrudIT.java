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
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.http.MockResponse;
import org.elasticsearch.xpack.core.inference.action.PutCCMConfigurationAction;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceServiceSettings;
import org.elasticsearch.xpack.inference.services.elastic.ccm.CCMFeatureFlag;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.IOException;

import static org.elasticsearch.xpack.inference.action.TransportPutCCMConfigurationActionTests.ERROR_MESSAGE;
import static org.elasticsearch.xpack.inference.action.TransportPutCCMConfigurationActionTests.ERROR_RESPONSE;
import static org.elasticsearch.xpack.inference.external.http.retry.RetryingHttpSender.MAX_RETRIES;
import static org.elasticsearch.xpack.inference.rest.Paths.INFERENCE_CCM_PATH;
import static org.elasticsearch.xpack.inference.services.elastic.ccm.CCMSettings.CCM_SUPPORTED_ENVIRONMENT;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class CCMCrudIT extends CCMRestBaseIT {

    private static final MockElasticInferenceServiceAuthorizationServer mockEISServer =
        new MockElasticInferenceServiceAuthorizationServer();

    private static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .setting("xpack.license.self_generated.type", "basic")
        .setting("xpack.security.enabled", "true")
        .setting(CCM_SUPPORTED_ENVIRONMENT.getKey(), "true")
        .setting(ElasticInferenceServiceSettings.ELASTIC_INFERENCE_SERVICE_URL.getKey(), mockEISServer::getUrl)
        .setting(ElasticInferenceServiceSettings.PERIODIC_AUTHORIZATION_ENABLED.getKey(), "false")
        // TODO disable the authorization task so it doesn't try to call the mock server
        .user("x_pack_rest_user", "x-pack-test-password")
        .build();

    // The reason we're doing this is to make sure the mock server is initialized first so we can get the address before communicating
    // it to the cluster as a setting.
    // Note: @ClassRule is executed once for the entire test class
    @ClassRule
    public static TestRule ruleChain = RuleChain.outerRule(mockEISServer).around(cluster);

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue("x_pack_rest_user", new SecureString("x-pack-test-password".toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    @BeforeClass
    public static void classSetup() {
        assumeTrue("CCM is behind a feature flag and snapshot only right now", CCMFeatureFlag.FEATURE_FLAG.isEnabled());
    }

    @After
    public void cleanup() {
        try {
            // Disable CCM after each test to ensure a clean state
            client().performRequest(new Request(DELETE_METHOD, INFERENCE_CCM_PATH));
        } catch (Exception e) {
            // ignore failures
        }
    }

    public void testEnablesCCM_Succeeds() throws IOException {
        mockEISServer.enqueueAuthorizeAllModelsResponse();
        var response = putCCMConfiguration(ENABLE_CCM_REQUEST);

        assertTrue(response.isEnabled());
    }

    public void testEnablesCCM_Fails_IfAuthValidationFails401() throws IOException {
        var webServer = mockEISServer.getWebServer();
        webServer.enqueue(new MockResponse().setResponseCode(RestStatus.UNAUTHORIZED.getStatus()).setBody(ERROR_RESPONSE));

        var exception = expectThrows(ResponseException.class, () -> putRawRequest(INFERENCE_CCM_PATH, ENABLE_CCM_REQUEST));
        assertThat(exception.getMessage(), containsString(Strings.format(ERROR_MESSAGE)));
        assertThat(exception.getResponse().getStatusLine().getStatusCode(), is(RestStatus.UNAUTHORIZED.getStatus()));

        assertFalse(getCCMConfiguration().isEnabled());
    }

    public void testEnablesCCM_Fails_IfAuthValidationFails400() throws IOException {
        var webServer = mockEISServer.getWebServer();
        webServer.enqueue(new MockResponse().setResponseCode(RestStatus.BAD_REQUEST.getStatus()).setBody(ERROR_RESPONSE));

        var exception = expectThrows(ResponseException.class, () -> putRawRequest(INFERENCE_CCM_PATH, ENABLE_CCM_REQUEST));
        assertThat(exception.getMessage(), containsString(Strings.format(ERROR_MESSAGE)));
        assertThat(exception.getResponse().getStatusLine().getStatusCode(), is(RestStatus.BAD_REQUEST.getStatus()));

        assertFalse(getCCMConfiguration().isEnabled());
    }

    public void testEnablesCCM_Fails_IfAuthValidationFails500() throws IOException {
        // 500 errors are retried so we need to queue up multiple responses
        queueResponsesToHandleRetries(
            new MockResponse().setResponseCode(RestStatus.INTERNAL_SERVER_ERROR.getStatus()).setBody(ERROR_RESPONSE)
        );

        var exception = expectThrows(ResponseException.class, () -> putRawRequest(INFERENCE_CCM_PATH, ENABLE_CCM_REQUEST));
        assertThat(exception.getMessage(), containsString(Strings.format(ERROR_MESSAGE)));
        // 5xx errors are transformed by the response handler to a 400 error to avoid triggering alerts
        assertThat(exception.getResponse().getStatusLine().getStatusCode(), is(RestStatus.BAD_REQUEST.getStatus()));

        assertFalse(getCCMConfiguration().isEnabled());
    }

    private void queueResponsesToHandleRetries(MockResponse response) {
        for (int i = 0; i < MAX_RETRIES; i++) {
            mockEISServer.getWebServer().enqueue(response);
        }
    }

    public void testEnablesCCMTwice_Succeeds() throws IOException {
        mockEISServer.enqueueAuthorizeAllModelsResponse();
        var response = putCCMConfiguration(ENABLE_CCM_REQUEST);

        assertTrue(response.isEnabled());

        mockEISServer.enqueueAuthorizeAllModelsResponse();
        response = putCCMConfiguration(
            PutCCMConfigurationAction.Request.createEnabled("other_key", TimeValue.THIRTY_SECONDS, TimeValue.THIRTY_SECONDS)
        );

        assertTrue(response.isEnabled());
    }

    public void testEnablesCCM_WithExtraFields_Fails() {
        var extraField = "extra_field";
        var requestBody = Strings.format("""
            {
              "api_key": "key",
              "%s": "extra_value"
            }
            """, extraField);
        var exception = expectThrows(ResponseException.class, () -> putRawRequest(INFERENCE_CCM_PATH, requestBody));

        assertThat(exception.getMessage(), containsString(Strings.format("unknown field [%s]", extraField)));
        assertThat(exception.getResponse().getStatusLine().getStatusCode(), is(RestStatus.BAD_REQUEST.getStatus()));
    }

    public void testEnableCCM_WithNullApiKey_ThrowsException() {
        var request = new PutCCMConfigurationAction.Request(null, TimeValue.THIRTY_SECONDS, TimeValue.THIRTY_SECONDS);

        var exception = expectThrows(IOException.class, () -> putCCMConfiguration(request));
        assertThat(exception.getMessage(), containsString("The [api_key] field cannot be an empty string or null"));
    }

    public void testEnableCCM_WithoutBody_ThrowsException() {
        var request = new Request(PUT_METHOD, INFERENCE_CCM_PATH);

        var exception = expectThrows(ResponseException.class, () -> client().performRequest(request));
        assertThat(exception.getMessage(), containsString("The body must be specified"));
    }

    public void testDisableCCM_Succeeds() throws IOException {
        var response = deleteCCMConfiguration();
        assertFalse(response.isEnabled());
    }

    public void testGetCCMConfiguration_WhenCCMDisabled_ReturnsDisabled() throws IOException {
        assertFalse(getCCMConfiguration().isEnabled());
    }

    public void testEnablesCCM_ThenDisable() throws IOException {
        mockEISServer.enqueueAuthorizeAllModelsResponse();
        var response = putCCMConfiguration(ENABLE_CCM_REQUEST);

        assertTrue(response.isEnabled());
        assertTrue(getCCMConfiguration().isEnabled());

        response = deleteCCMConfiguration();

        assertFalse(response.isEnabled());
        assertFalse(getCCMConfiguration().isEnabled());
    }
}

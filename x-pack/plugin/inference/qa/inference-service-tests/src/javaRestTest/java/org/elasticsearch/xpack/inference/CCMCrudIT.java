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
import org.elasticsearch.xpack.core.inference.action.PutCCMConfigurationAction;
import org.junit.After;
import org.junit.ClassRule;

import java.io.IOException;

import static org.elasticsearch.xpack.inference.rest.Paths.INFERENCE_CCM_PATH;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class CCMCrudIT extends CCMBaseIT {

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .setting("xpack.license.self_generated.type", "basic")
        .setting("xpack.security.enabled", "true")
        .setting("xpack.inference.elastic.allow_configuring_ccm", "true")
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

    public void testEnablesCCMTwice_Succeeds() throws IOException {
        var response = putCCMConfiguration(
            PutCCMConfigurationAction.Request.createEnabled(
                new SecureString("key".toCharArray()),
                TimeValue.THIRTY_SECONDS,
                TimeValue.THIRTY_SECONDS
            )
        );

        assertTrue(response.isEnabled());

        response = putCCMConfiguration(
            PutCCMConfigurationAction.Request.createEnabled(
                new SecureString("other_key".toCharArray()),
                TimeValue.THIRTY_SECONDS,
                TimeValue.THIRTY_SECONDS
            )
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

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 *
 * this file has been contributed to by a Generative AI
 */

package org.elasticsearch.xpack.inference;

import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.xpack.core.inference.action.PutCCMConfigurationAction;
import org.junit.ClassRule;

import static org.elasticsearch.xpack.inference.services.elastic.ccm.CCMFeature.CCM_FORBIDDEN_EXCEPTION;
import static org.elasticsearch.xpack.inference.services.elastic.ccm.CCMSettings.CCM_SUPPORTED_ENVIRONMENT;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class CCMCrudForbiddenIT extends CCMRestBaseIT {

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .setting("xpack.license.self_generated.type", "basic")
        .setting("xpack.security.enabled", "true")
        .setting(CCM_SUPPORTED_ENVIRONMENT.getKey(), "false")
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

    public void testRequestToEnableCCM_ReturnsForbidden() {
        var exception = expectThrows(ResponseException.class, () -> putCCMConfiguration(ENABLE_CCM_REQUEST));
        assertForbidden(exception);
    }

    private void assertForbidden(ResponseException exception) {
        assertThat(exception.getResponse().getStatusLine().getStatusCode(), is(RestStatus.FORBIDDEN.getStatus()));
        assertThat(exception.getMessage(), containsString(CCM_FORBIDDEN_EXCEPTION.getMessage()));
    }

    public void testEnableCCM_WithNullApiKey_NullEnabled_ReturnsForbidden() {
        var request = new PutCCMConfigurationAction.Request(null, TimeValue.THIRTY_SECONDS, TimeValue.THIRTY_SECONDS);

        var exception = expectThrows(ResponseException.class, () -> putCCMConfiguration(request));
        assertForbidden(exception);
    }

    public void testGetCCMConfiguration_ReturnsForbidden() {
        var exception = expectThrows(ResponseException.class, CCMRestBaseIT::getCCMConfiguration);
        assertForbidden(exception);
    }

    public void testDeleteCCMConfiguration_ReturnsForbidden() {
        var exception = expectThrows(ResponseException.class, CCMRestBaseIT::deleteCCMConfiguration);
        assertForbidden(exception);
    }
}

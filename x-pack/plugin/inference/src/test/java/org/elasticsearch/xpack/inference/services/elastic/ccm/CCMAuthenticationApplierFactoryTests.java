/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.ccm;

import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpGet;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.TestPlainActionFuture;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;

import static org.elasticsearch.xpack.inference.external.request.RequestUtils.bearerToken;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CCMAuthenticationApplierFactoryTests extends ESTestCase {

    public static CCMAuthenticationApplierFactory createNoopApplierFactory() {
        var mockFactory = mock(CCMAuthenticationApplierFactory.class);
        doAnswer(invocation -> {
            ActionListener<CCMAuthenticationApplierFactory.AuthApplier> listener = invocation.getArgument(0);
            listener.onResponse(CCMAuthenticationApplierFactory.NOOP_APPLIER);
            return Void.TYPE;
        }).when(mockFactory).getAuthenticationApplier(any());

        return mockFactory;
    }

    public static CCMAuthenticationApplierFactory createApplierFactory(String secret) {
        var mockFactory = mock(CCMAuthenticationApplierFactory.class);
        doAnswer(invocation -> {
            ActionListener<CCMAuthenticationApplierFactory.AuthApplier> listener = invocation.getArgument(0);
            listener.onResponse(new CCMAuthenticationApplierFactory.AuthenticationHeaderApplier(secret));
            return Void.TYPE;
        }).when(mockFactory).getAuthenticationApplier(any());

        return mockFactory;
    }

    public void testNoopApplierReturnsSameRequest() {
        var applier = CCMAuthenticationApplierFactory.NOOP_APPLIER;
        var request = new HttpGet("http://localhost");
        var result = applier.apply(request);
        assertThat(result, sameInstance(request));
    }

    public void testAuthenticationHeaderApplierSetsAuthorizationHeader() {
        var secret = "my-secret";
        var applier = new CCMAuthenticationApplierFactory.AuthenticationHeaderApplier(secret);
        var request = new HttpGet("http://localhost");
        applier.apply(request);
        assertThat(request.getFirstHeader(HttpHeaders.AUTHORIZATION).getValue(), is(bearerToken(secret)));
    }

    public void testGetAuthenticationApplier_ReturnsNoopWhenConfiguringCCMIsDisabled() {
        var ccmFeature = mock(CCMFeature.class);
        when(ccmFeature.isCcmSupportedEnvironment()).thenReturn(false);
        var ccmService = mock(CCMService.class);

        var factory = new CCMAuthenticationApplierFactory(ccmFeature, ccmService);
        var listener = new TestPlainActionFuture<CCMAuthenticationApplierFactory.AuthApplier>();
        factory.getAuthenticationApplier(listener);

        assertThat(listener.actionGet(TimeValue.THIRTY_SECONDS), sameInstance(CCMAuthenticationApplierFactory.NOOP_APPLIER));
    }

    public void testGetAuthenticationApplier_ReturnsFailure_WhenConfiguringCCMIsEnabled_ButHasNotBeenConfiguredYet() {
        var ccmFeature = mock(CCMFeature.class);
        when(ccmFeature.isCcmSupportedEnvironment()).thenReturn(true);

        var ccmService = mock(CCMService.class);
        doAnswer(invocation -> {
            ActionListener<Boolean> listener = invocation.getArgument(0);
            listener.onResponse(false);
            return Void.TYPE;
        }).when(ccmService).isEnabled(any());

        var factory = new CCMAuthenticationApplierFactory(ccmFeature, ccmService);
        var listener = new TestPlainActionFuture<CCMAuthenticationApplierFactory.AuthApplier>();
        factory.getAuthenticationApplier(listener);

        var exception = expectThrows(ElasticsearchStatusException.class, () -> listener.actionGet(TimeValue.THIRTY_SECONDS));
        assertThat(
            exception.getMessage(),
            containsString(
                "Cloud connected mode is not configured, please configure it using PUT _inference/_ccm "
                    + "before accessing the Elastic Inference Service."
            )
        );
    }

    public void testGetAuthenticationApplier_ReturnsApiKey_WhenConfiguringCCMIsEnabled_AndSet() {
        var secret = "secret";

        var ccmFeature = mock(CCMFeature.class);
        when(ccmFeature.isCcmSupportedEnvironment()).thenReturn(true);

        var ccmService = mock(CCMService.class);
        doAnswer(invocation -> {
            ActionListener<Boolean> listener = invocation.getArgument(0);
            listener.onResponse(true);
            return Void.TYPE;
        }).when(ccmService).isEnabled(any());
        doAnswer(invocation -> {
            ActionListener<CCMModel> listener = invocation.getArgument(0);
            listener.onResponse(new CCMModel(new SecureString(secret.toCharArray())));
            return Void.TYPE;
        }).when(ccmService).getConfiguration(any());

        var factory = new CCMAuthenticationApplierFactory(ccmFeature, ccmService);
        var listener = new TestPlainActionFuture<CCMAuthenticationApplierFactory.AuthApplier>();
        factory.getAuthenticationApplier(listener);

        var applier = listener.actionGet(TimeValue.THIRTY_SECONDS);
        assertThat(applier, is(new CCMAuthenticationApplierFactory.AuthenticationHeaderApplier(secret)));
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.action.apikey.ApiKey;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationResult;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.authc.ApiKeyService.ApiKeyCredentials;
import org.elasticsearch.xpack.security.authc.AuthenticationService.AuditableRequest;

import static org.elasticsearch.test.ActionListenerUtils.anyActionListener;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ApiKeyAuthenticatorTests extends ESTestCase {

    public void testAuditingOnAuthenticationTermination() {
        final ApiKeyService apiKeyService = mock(ApiKeyService.class);
        final ApiKeyAuthenticator apiKeyAuthenticator = new ApiKeyAuthenticator(apiKeyService, randomAlphaOfLengthBetween(3, 8));

        final Authenticator.Context context = mock(Authenticator.Context.class);

        final ApiKeyCredentials apiKeyCredentials = new ApiKeyCredentials(
            randomAlphaOfLength(20),
            new SecureString(randomAlphaOfLength(20).toCharArray()),
            randomFrom(ApiKey.Type.values())
        );
        when(context.getMostRecentAuthenticationToken()).thenReturn(apiKeyCredentials);
        final ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        when(context.getThreadContext()).thenReturn(threadContext);
        final AuditableRequest auditableRequest = mock(AuditableRequest.class);
        when(context.getRequest()).thenReturn(auditableRequest);

        final Exception terminationError = randomFrom(new ElasticsearchException("termination error"), null);
        doAnswer(invocation -> {
            final ActionListener<AuthenticationResult<User>> listener = invocation.getArgument(2);
            listener.onResponse(AuthenticationResult.terminate("terminated by ApiKeyService", terminationError));
            return null;
        }).when(apiKeyService).tryAuthenticate(same(threadContext), same(apiKeyCredentials), anyActionListener());

        final PlainActionFuture<AuthenticationResult<Authentication>> future = new PlainActionFuture<>();
        apiKeyAuthenticator.authenticate(context, future);

        final Exception e = expectThrows(Exception.class, future::actionGet);
        verify(auditableRequest).exceptionProcessingRequest(any(Exception.class), same(apiKeyCredentials));
        if (terminationError == null) {
            assertThat(e, instanceOf(ElasticsearchSecurityException.class));
            assertThat(e.getMessage(), containsString("terminated by ApiKeyService"));
        } else {
            assertThat(e, sameInstance(terminationError));
        }
    }

}

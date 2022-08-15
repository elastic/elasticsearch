/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.action.user;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.action.user.GetUserPrivilegesRequest;
import org.elasticsearch.xpack.core.security.action.user.GetUserPrivilegesRequestBuilder;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationTestHelper;
import org.elasticsearch.xpack.security.authz.AuthorizationService;
import org.junit.Before;

import static org.elasticsearch.test.ActionListenerUtils.anyActionListener;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransportGetUserPrivilegesActionTests extends ESTestCase {

    private SecurityContext securityContext;
    private AuthorizationService authorizationService;
    private TransportGetUserPrivilegesAction transportGetUserPrivilegesAction;

    @Before
    public void init() {
        securityContext = mock(SecurityContext.class);
        authorizationService = mock(AuthorizationService.class);
        transportGetUserPrivilegesAction = new TransportGetUserPrivilegesAction(
            mock(TransportService.class),
            mock(ActionFilters.class),
            authorizationService,
            securityContext
        );
    }

    public void testGetPrivilegesForApiKeyCanThrowElasticsearchSecurityExceptionWith400Status() {
        final Authentication authentication = AuthenticationTestHelper.builder().apiKey().build();
        when(securityContext.getAuthentication()).thenReturn(authentication);
        when(securityContext.requireUser()).thenReturn(authentication.getEffectiveSubject().getUser());
        final GetUserPrivilegesRequest request = new GetUserPrivilegesRequestBuilder(mock(Client.class)).username(
            authentication.getEffectiveSubject().getUser().principal()
        ).request();
        doThrow(mock(UnsupportedOperationException.class)).when(authorizationService)
            .retrieveUserPrivileges(any(), any(), anyActionListener());

        final ElasticsearchSecurityException e = expectThrows(
            ElasticsearchSecurityException.class,
            () -> transportGetUserPrivilegesAction.doExecute(mock(Task.class), request, new PlainActionFuture<>())
        );
        assertThat(e.status(), equalTo(RestStatus.BAD_REQUEST));
        assertThat(
            e.getMessage(),
            equalTo(
                "Cannot retrieve privileges for API keys with assigned role descriptors. "
                    + "Please use the Get API key information API https://ela.st/es-api-get-api-key"
            )
        );
    }

    public void testGetPrivilegesThrowExceptionUnwrappedIfItIsNotUnsupportedOperation() {
        final Authentication authentication = AuthenticationTestHelper.builder().build();
        when(securityContext.getAuthentication()).thenReturn(authentication);
        when(securityContext.requireUser()).thenReturn(authentication.getEffectiveSubject().getUser());
        final GetUserPrivilegesRequest request = new GetUserPrivilegesRequestBuilder(mock(Client.class)).username(
            authentication.getEffectiveSubject().getUser().principal()
        ).request();
        final RuntimeException e = mock(
            randomFrom(IllegalStateException.class, ElasticsearchException.class, IllegalArgumentException.class)
        );
        doThrow(e).when(authorizationService).retrieveUserPrivileges(any(), any(), anyActionListener());

        assertThat(
            expectThrows(
                RuntimeException.class,
                () -> transportGetUserPrivilegesAction.doExecute(mock(Task.class), request, new PlainActionFuture<>())
            ),
            sameInstance(e)
        );
    }

    public void testGetPrivilegeThrowExceptionUnwrappedIfAuthenticationIsNotApiKey() {
        final Authentication authentication = randomValueOtherThanMany(
            Authentication::isApiKey,
            () -> AuthenticationTestHelper.builder().build()
        );
        when(securityContext.getAuthentication()).thenReturn(authentication);
        when(securityContext.requireUser()).thenReturn(authentication.getEffectiveSubject().getUser());
        final GetUserPrivilegesRequest request = new GetUserPrivilegesRequestBuilder(mock(Client.class)).username(
            authentication.getEffectiveSubject().getUser().principal()
        ).request();
        final RuntimeException e = mock(
            randomFrom(
                UnsupportedOperationException.class,
                IllegalStateException.class,
                ElasticsearchException.class,
                IllegalArgumentException.class
            )
        );
        doThrow(e).when(authorizationService).retrieveUserPrivileges(any(), any(), anyActionListener());

        assertThat(
            expectThrows(
                RuntimeException.class,
                () -> transportGetUserPrivilegesAction.doExecute(mock(Task.class), request, new PlainActionFuture<>())
            ),
            sameInstance(e)
        );
    }
}

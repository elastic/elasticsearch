/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authz.interceptor;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.shrink.ResizeAction;
import org.elasticsearch.action.admin.indices.shrink.ResizeRequest;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.license.MockLicenseState;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.Authentication.RealmRef;
import org.elasticsearch.xpack.core.security.authc.AuthenticationTestHelper;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine.AuthorizationResult;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine.EmptyAuthorizationInfo;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine.RequestInfo;
import org.elasticsearch.xpack.core.security.authz.accesscontrol.IndicesAccessControl;
import org.elasticsearch.xpack.core.security.authz.permission.DocumentPermissions;
import org.elasticsearch.xpack.core.security.authz.permission.FieldPermissions;
import org.elasticsearch.xpack.core.security.authz.permission.FieldPermissionsDefinition;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.Security;
import org.elasticsearch.xpack.security.audit.AuditTrailService;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.test.ActionListenerUtils.anyActionListener;
import static org.elasticsearch.xpack.core.security.SecurityField.DOCUMENT_LEVEL_SECURITY_FEATURE;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ResizeRequestInterceptorTests extends ESTestCase {

    public void testResizeRequestInterceptorThrowsWhenFLSDLSEnabled() {
        checkResizeWithDlsFlsConfigured(
            true,
            "Resize requests are not allowed for users when field or document level security is enabled on the source index"
        );
    }

    public void testResizeRequestInterceptorWorksAsNormalWhenFLSDLSDisabled() {
        checkResizeWithDlsFlsConfigured(
            false,
            "Resizing an index is not allowed when the target index has more permissions than the source index"
        );
    }

    @SuppressWarnings("unchecked")
    public void checkResizeWithDlsFlsConfigured(boolean dlsFlsFeatureEnabled, String expectedErrorMessage) {
        MockLicenseState licenseState = mock(MockLicenseState.class);
        when(licenseState.copyCurrentLicenseState()).thenReturn(licenseState);
        when(licenseState.isAllowed(Security.AUDITING_FEATURE)).thenReturn(true);
        when(licenseState.isAllowed(DOCUMENT_LEVEL_SECURITY_FEATURE)).thenReturn(true);
        ThreadPool threadPool = mock(ThreadPool.class);
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        when(threadPool.getThreadContext()).thenReturn(threadContext);
        AuditTrailService auditTrailService = new AuditTrailService(null, licenseState);
        final Authentication authentication = AuthenticationTestHelper.builder()
            .user(new User("john", "role"))
            .realmRef(new RealmRef("realm", "type", "node", null))
            .build();
        final FieldPermissions fieldPermissions;
        final boolean useFls = randomBoolean();
        if (useFls) {
            fieldPermissions = new FieldPermissions(new FieldPermissionsDefinition(new String[] { "foo" }, null));
        } else {
            fieldPermissions = FieldPermissions.DEFAULT;
        }
        final boolean useDls = (useFls == false) || randomBoolean();
        final Set<BytesReference> queries;
        if (useDls) {
            queries = Collections.singleton(new BytesArray(randomAlphaOfLengthBetween(2, 8)));
        } else {
            queries = null;
        }
        IndicesAccessControl accessControl = new IndicesAccessControl(
            true,
            Collections.singletonMap(
                "foo",
                new IndicesAccessControl.IndexAccessControl(
                    fieldPermissions,
                    (useDls) ? DocumentPermissions.filteredBy(queries) : DocumentPermissions.allowAll()
                )
            )
        );
        new SecurityContext(Settings.EMPTY, threadContext).putIndicesAccessControl(accessControl);

        ResizeRequestInterceptor resizeRequestInterceptor = new ResizeRequestInterceptor(
            threadPool,
            licenseState,
            auditTrailService,
            dlsFlsFeatureEnabled
        );

        PlainActionFuture<Void> plainActionFuture = new PlainActionFuture<>();
        RequestInfo requestInfo = new RequestInfo(authentication, new ResizeRequest("bar", "foo"), ResizeAction.NAME, null);
        AuthorizationEngine mockEngine = mock(AuthorizationEngine.class);
        doAnswer(invocationOnMock -> {
            ActionListener<AuthorizationResult> listener = (ActionListener<AuthorizationResult>) invocationOnMock.getArguments()[3];
            listener.onResponse(AuthorizationResult.deny());
            return null;
        }).when(mockEngine)
            .validateIndexPermissionsAreSubset(eq(requestInfo), eq(EmptyAuthorizationInfo.INSTANCE), anyMap(), anyActionListener());
        ElasticsearchSecurityException securityException = expectThrows(ElasticsearchSecurityException.class, () -> {
            resizeRequestInterceptor.intercept(requestInfo, mockEngine, EmptyAuthorizationInfo.INSTANCE).addListener(plainActionFuture);
            plainActionFuture.actionGet();
        });
        assertEquals(expectedErrorMessage, securityException.getMessage());
    }

    @SuppressWarnings("unchecked")
    public void testResizeRequestInterceptorThrowsWhenTargetHasGreaterPermissions() throws Exception {
        MockLicenseState licenseState = mock(MockLicenseState.class);
        when(licenseState.copyCurrentLicenseState()).thenReturn(licenseState);
        when(licenseState.isAllowed(Security.AUDITING_FEATURE)).thenReturn(true);
        when(licenseState.isAllowed(DOCUMENT_LEVEL_SECURITY_FEATURE)).thenReturn(true);
        ThreadPool threadPool = mock(ThreadPool.class);
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        when(threadPool.getThreadContext()).thenReturn(threadContext);
        AuditTrailService auditTrailService = new AuditTrailService(null, licenseState);
        final Authentication authentication = AuthenticationTestHelper.builder()
            .user(new User("john", "role"))
            .realmRef(new RealmRef("realm", "type", "node", null))
            .build();
        IndicesAccessControl accessControl = new IndicesAccessControl(true, Collections.emptyMap());
        new SecurityContext(Settings.EMPTY, threadContext).putIndicesAccessControl(accessControl);
        ResizeRequestInterceptor resizeRequestInterceptor = new ResizeRequestInterceptor(threadPool, licenseState, auditTrailService, true);

        AuthorizationEngine mockEngine = mock(AuthorizationEngine.class);
        {
            PlainActionFuture<Void> plainActionFuture = new PlainActionFuture<>();
            RequestInfo requestInfo = new RequestInfo(authentication, new ResizeRequest("target", "source"), ResizeAction.NAME, null);
            doAnswer(invocationOnMock -> {
                ActionListener<AuthorizationResult> listener = (ActionListener<AuthorizationResult>) invocationOnMock.getArguments()[3];
                listener.onResponse(AuthorizationResult.deny());
                return null;
            }).when(mockEngine)
                .validateIndexPermissionsAreSubset(
                    eq(requestInfo),
                    eq(EmptyAuthorizationInfo.INSTANCE),
                    any(Map.class),
                    anyActionListener()
                );
            ElasticsearchSecurityException securityException = expectThrows(ElasticsearchSecurityException.class, () -> {
                resizeRequestInterceptor.intercept(requestInfo, mockEngine, EmptyAuthorizationInfo.INSTANCE).addListener(plainActionFuture);
                plainActionFuture.actionGet();
            });
            assertEquals(
                "Resizing an index is not allowed when the target index has more permissions than the source index",
                securityException.getMessage()
            );
        }

        // swap target and source for success
        {
            PlainActionFuture<Void> plainActionFuture = new PlainActionFuture<>();
            RequestInfo requestInfo = new RequestInfo(authentication, new ResizeRequest("source", "target"), ResizeAction.NAME, null);
            doAnswer(invocationOnMock -> {
                ActionListener<AuthorizationResult> listener = (ActionListener<AuthorizationResult>) invocationOnMock.getArguments()[3];
                listener.onResponse(AuthorizationResult.granted());
                return null;
            }).when(mockEngine)
                .validateIndexPermissionsAreSubset(
                    eq(requestInfo),
                    eq(EmptyAuthorizationInfo.INSTANCE),
                    any(Map.class),
                    anyActionListener()
                );
            resizeRequestInterceptor.intercept(requestInfo, mockEngine, EmptyAuthorizationInfo.INSTANCE).addListener(plainActionFuture);
            plainActionFuture.actionGet();
        }
    }
}

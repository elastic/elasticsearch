/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authz.interceptor;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesAction;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.license.MockLicenseState;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.Authentication.RealmRef;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine.AuthorizationResult;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine.EmptyAuthorizationInfo;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine.RequestInfo;
import org.elasticsearch.xpack.core.security.authz.AuthorizationServiceField;
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

public class IndicesAliasesRequestInterceptorTests extends ESTestCase {

    @SuppressWarnings("unchecked")
    public void testInterceptorThrowsWhenFLSDLSEnabled() {
        MockLicenseState licenseState = mock(MockLicenseState.class);
        when(licenseState.copyCurrentLicenseState()).thenReturn(licenseState);
        when(licenseState.isAllowed(Security.AUDITING_FEATURE)).thenReturn(true);
        when(licenseState.isAllowed(DOCUMENT_LEVEL_SECURITY_FEATURE)).thenReturn(true);
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        AuditTrailService auditTrailService = new AuditTrailService(Collections.emptyList(), licenseState);
        Authentication authentication = new Authentication(
            new User("john", "role"),
            new RealmRef("auth_name", "auth_type", "node"),
            new RealmRef("look_name", "look_type", "node")
        );
        final FieldPermissions fieldPermissions;
        final boolean useFls = randomBoolean();
        if (useFls) {
            fieldPermissions = new FieldPermissions(new FieldPermissionsDefinition(new String[] { "foo" }, null));
        } else {
            fieldPermissions = new FieldPermissions();
        }
        final boolean useDls = (useFls == false) || randomBoolean();
        final Set<BytesReference> queries;
        if (useDls) {
            queries = Collections.singleton(new BytesArray(randomAlphaOfLengthBetween(2, 8)));
        } else {
            queries = null;
        }
        final String action = IndicesAliasesAction.NAME;
        IndicesAccessControl accessControl = new IndicesAccessControl(
            true,
            Collections.singletonMap(
                "foo",
                new IndicesAccessControl.IndexAccessControl(
                    true,
                    fieldPermissions,
                    (useDls) ? DocumentPermissions.filteredBy(queries) : DocumentPermissions.allowAll()
                )
            )
        );
        threadContext.putTransient(AuthorizationServiceField.INDICES_PERMISSIONS_KEY, accessControl);

        IndicesAliasesRequestInterceptor interceptor = new IndicesAliasesRequestInterceptor(threadContext, licenseState, auditTrailService);

        IndicesAliasesRequest indicesAliasesRequest = new IndicesAliasesRequest();
        if (randomBoolean()) {
            indicesAliasesRequest.addAliasAction(IndicesAliasesRequest.AliasActions.remove().index("bar").alias(randomAlphaOfLength(4)));
        }
        indicesAliasesRequest.addAliasAction(IndicesAliasesRequest.AliasActions.add().index("foo").alias(randomAlphaOfLength(4)));
        if (randomBoolean()) {
            indicesAliasesRequest.addAliasAction(IndicesAliasesRequest.AliasActions.removeIndex().index("foofoo"));
        }
        PlainActionFuture<Void> plainActionFuture = new PlainActionFuture<>();
        RequestInfo requestInfo = new RequestInfo(authentication, indicesAliasesRequest, action, null);
        AuthorizationEngine mockEngine = mock(AuthorizationEngine.class);
        doAnswer(invocationOnMock -> {
            ActionListener<AuthorizationResult> listener = (ActionListener<AuthorizationResult>) invocationOnMock.getArguments()[3];
            listener.onResponse(AuthorizationResult.deny());
            return null;
        }).when(mockEngine)
            .validateIndexPermissionsAreSubset(eq(requestInfo), eq(EmptyAuthorizationInfo.INSTANCE), anyMap(), anyActionListener());
        ElasticsearchSecurityException securityException = expectThrows(ElasticsearchSecurityException.class, () -> {
            interceptor.intercept(requestInfo, mockEngine, EmptyAuthorizationInfo.INSTANCE, plainActionFuture);
            plainActionFuture.actionGet();
        });
        assertEquals(
            "Alias requests are not allowed for users who have field or document level security enabled on one of the indices",
            securityException.getMessage()
        );
    }

    @SuppressWarnings("unchecked")
    public void testInterceptorThrowsWhenTargetHasGreaterPermissions() throws Exception {
        MockLicenseState licenseState = mock(MockLicenseState.class);
        when(licenseState.copyCurrentLicenseState()).thenReturn(licenseState);
        when(licenseState.isAllowed(Security.AUDITING_FEATURE)).thenReturn(true);
        when(licenseState.isAllowed(DOCUMENT_LEVEL_SECURITY_FEATURE)).thenReturn(randomBoolean());
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        AuditTrailService auditTrailService = new AuditTrailService(Collections.emptyList(), licenseState);
        Authentication authentication = new Authentication(
            new User("john", "role"),
            new RealmRef("auth_name", "auth_type", "node"),
            new RealmRef("look_name", "look_type", "node")
        );
        final String action = IndicesAliasesAction.NAME;
        IndicesAccessControl accessControl = new IndicesAccessControl(true, Collections.emptyMap());
        threadContext.putTransient(AuthorizationServiceField.INDICES_PERMISSIONS_KEY, accessControl);
        IndicesAliasesRequestInterceptor interceptor = new IndicesAliasesRequestInterceptor(threadContext, licenseState, auditTrailService);

        final IndicesAliasesRequest indicesAliasesRequest = new IndicesAliasesRequest();
        if (randomBoolean()) {
            indicesAliasesRequest.addAliasAction(IndicesAliasesRequest.AliasActions.remove().index("bar").alias(randomAlphaOfLength(4)));
        }
        indicesAliasesRequest.addAliasAction(IndicesAliasesRequest.AliasActions.add().index("index").alias("alias"));
        if (randomBoolean()) {
            indicesAliasesRequest.addAliasAction(IndicesAliasesRequest.AliasActions.removeIndex().index("foofoo"));
        }

        AuthorizationEngine mockEngine = mock(AuthorizationEngine.class);
        {
            PlainActionFuture<Void> plainActionFuture = new PlainActionFuture<>();
            RequestInfo requestInfo = new RequestInfo(authentication, indicesAliasesRequest, action, null);
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
                interceptor.intercept(requestInfo, mockEngine, EmptyAuthorizationInfo.INSTANCE, plainActionFuture);
                plainActionFuture.actionGet();
            });
            assertEquals(
                "Adding an alias is not allowed when the alias has more permissions than any of the indices",
                securityException.getMessage()
            );
        }

        // swap target and source for success
        final IndicesAliasesRequest successRequest = new IndicesAliasesRequest();
        if (randomBoolean()) {
            successRequest.addAliasAction(IndicesAliasesRequest.AliasActions.remove().index("bar").alias(randomAlphaOfLength(4)));
        }
        successRequest.addAliasAction(IndicesAliasesRequest.AliasActions.add().index("alias").alias("index"));
        if (randomBoolean()) {
            successRequest.addAliasAction(IndicesAliasesRequest.AliasActions.removeIndex().index("foofoo"));
        }

        {
            PlainActionFuture<Void> plainActionFuture = new PlainActionFuture<>();
            RequestInfo requestInfo = new RequestInfo(authentication, successRequest, action, null);
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
            interceptor.intercept(requestInfo, mockEngine, EmptyAuthorizationInfo.INSTANCE, plainActionFuture);
            plainActionFuture.actionGet();
        }
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.action.interceptor;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.admin.indices.shrink.ResizeAction;
import org.elasticsearch.action.admin.indices.shrink.ResizeRequest;
import org.elasticsearch.action.admin.indices.shrink.ShrinkAction;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.Authentication.RealmRef;
import org.elasticsearch.xpack.core.security.authz.AuthorizationServiceField;
import org.elasticsearch.xpack.core.security.authz.accesscontrol.IndicesAccessControl;
import org.elasticsearch.xpack.core.security.authz.permission.FieldPermissions;
import org.elasticsearch.xpack.core.security.authz.permission.FieldPermissionsDefinition;
import org.elasticsearch.xpack.core.security.authz.permission.Role;
import org.elasticsearch.xpack.core.security.authz.privilege.IndexPrivilege;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.audit.AuditTrailService;

import java.util.Collections;
import java.util.Set;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ResizeRequestInterceptorTests extends ESTestCase {

    public void testResizeRequestInterceptorThrowsWhenFLSDLSEnabled() {
        XPackLicenseState licenseState = mock(XPackLicenseState.class);
        when(licenseState.copyCurrentLicenseState()).thenReturn(licenseState);
        when(licenseState.isAuthAllowed()).thenReturn(true);
        when(licenseState.isAuditingAllowed()).thenReturn(true);
        when(licenseState.isDocumentAndFieldLevelSecurityAllowed()).thenReturn(true);
        ThreadPool threadPool = mock(ThreadPool.class);
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        when(threadPool.getThreadContext()).thenReturn(threadContext);
        AuditTrailService auditTrailService = new AuditTrailService(Collections.emptyList(), licenseState);
        final Authentication authentication = new Authentication(new User("john", "role"), new RealmRef(null, null, null), null);
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
        Role role = Role.builder().add(fieldPermissions, queries, IndexPrivilege.ALL, "foo").build();
        final String action = randomFrom(ShrinkAction.NAME, ResizeAction.NAME);
        IndicesAccessControl accessControl = new IndicesAccessControl(true, Collections.singletonMap("foo",
                        new IndicesAccessControl.IndexAccessControl(true, fieldPermissions, queries)));
        threadContext.putTransient(AuthorizationServiceField.INDICES_PERMISSIONS_KEY, accessControl);

        ResizeRequestInterceptor resizeRequestInterceptor =
                new ResizeRequestInterceptor(threadPool, licenseState, auditTrailService);

        ElasticsearchSecurityException securityException = expectThrows(ElasticsearchSecurityException.class,
                () -> resizeRequestInterceptor.intercept(new ResizeRequest("bar", "foo"), authentication, role, action));
        assertEquals("Resize requests are not allowed for users when field or document level security is enabled on the source index",
                securityException.getMessage());
    }

    public void testResizeRequestInterceptorThrowsWhenTargetHasGreaterPermissions() throws Exception {
        XPackLicenseState licenseState = mock(XPackLicenseState.class);
        when(licenseState.copyCurrentLicenseState()).thenReturn(licenseState);
        when(licenseState.isAuthAllowed()).thenReturn(true);
        when(licenseState.isAuditingAllowed()).thenReturn(true);
        when(licenseState.isDocumentAndFieldLevelSecurityAllowed()).thenReturn(true);
        ThreadPool threadPool = mock(ThreadPool.class);
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        when(threadPool.getThreadContext()).thenReturn(threadContext);
        AuditTrailService auditTrailService = new AuditTrailService(Collections.emptyList(), licenseState);
        final Authentication authentication = new Authentication(new User("john", "role"), new RealmRef(null, null, null), null);
        Role role = Role.builder()
                .add(IndexPrivilege.ALL, "target")
                .add(IndexPrivilege.READ, "source")
                .build();
        final String action = randomFrom(ShrinkAction.NAME, ResizeAction.NAME);
        IndicesAccessControl accessControl = new IndicesAccessControl(true, Collections.emptyMap());
        threadContext.putTransient(AuthorizationServiceField.INDICES_PERMISSIONS_KEY, accessControl);
        ResizeRequestInterceptor resizeRequestInterceptor =
                new ResizeRequestInterceptor(threadPool, licenseState, auditTrailService);
        ElasticsearchSecurityException securityException = expectThrows(ElasticsearchSecurityException.class,
                () -> resizeRequestInterceptor.intercept(new ResizeRequest("target", "source"), authentication, role, action));
        assertEquals("Resizing an index is not allowed when the target index has more permissions than the source index",
                securityException.getMessage());

        // swap target and source for success
        resizeRequestInterceptor.intercept(new ResizeRequest("source", "target"), authentication, role, action);
    }
}

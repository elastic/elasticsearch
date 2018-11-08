/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.action.interceptor;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesAction;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.test.ESTestCase;
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

public class IndicesAliasesRequestInterceptorTests extends ESTestCase {

    public void testInterceptorThrowsWhenFLSDLSEnabled() {
        XPackLicenseState licenseState = mock(XPackLicenseState.class);
        when(licenseState.copyCurrentLicenseState()).thenReturn(licenseState);
        when(licenseState.isAuthAllowed()).thenReturn(true);
        when(licenseState.isAuditingAllowed()).thenReturn(true);
        when(licenseState.isDocumentAndFieldLevelSecurityAllowed()).thenReturn(true);
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        AuditTrailService auditTrailService = new AuditTrailService(Collections.emptyList(), licenseState);
        Authentication authentication = new Authentication(new User("john", "role"), new RealmRef(null, null, null),
                new RealmRef(null, null, null));
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
        final String action = IndicesAliasesAction.NAME;
        IndicesAccessControl accessControl = new IndicesAccessControl(true, Collections.singletonMap("foo",
                new IndicesAccessControl.IndexAccessControl(true, fieldPermissions, queries)));
        threadContext.putTransient(AuthorizationServiceField.INDICES_PERMISSIONS_KEY, accessControl);

        IndicesAliasesRequestInterceptor interceptor =
                new IndicesAliasesRequestInterceptor(threadContext, licenseState, auditTrailService);

        IndicesAliasesRequest indicesAliasesRequest = new IndicesAliasesRequest();
        if (randomBoolean()) {
            indicesAliasesRequest.addAliasAction(IndicesAliasesRequest.AliasActions.remove().index("bar").alias(randomAlphaOfLength(4)));
        }
        indicesAliasesRequest.addAliasAction(IndicesAliasesRequest.AliasActions.add().index("foo").alias(randomAlphaOfLength(4)));
        if (randomBoolean()) {
            indicesAliasesRequest.addAliasAction(IndicesAliasesRequest.AliasActions.removeIndex().index("foofoo"));
        }
        ElasticsearchSecurityException securityException = expectThrows(ElasticsearchSecurityException.class,
                () -> interceptor.intercept(indicesAliasesRequest, authentication, role, action));
        assertEquals("Alias requests are not allowed for users who have field or document level security enabled on one of the indices",
                securityException.getMessage());
    }

    public void testInterceptorThrowsWhenTargetHasGreaterPermissions() throws Exception {
        XPackLicenseState licenseState = mock(XPackLicenseState.class);
        when(licenseState.copyCurrentLicenseState()).thenReturn(licenseState);
        when(licenseState.isAuthAllowed()).thenReturn(true);
        when(licenseState.isAuditingAllowed()).thenReturn(true);
        when(licenseState.isDocumentAndFieldLevelSecurityAllowed()).thenReturn(true);
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        AuditTrailService auditTrailService = new AuditTrailService(Collections.emptyList(), licenseState);
        Authentication authentication = new Authentication(new User("john", "role"), new RealmRef(null, null, null),
                new RealmRef(null, null, null));
        Role role = Role.builder()
                .add(IndexPrivilege.ALL, "alias")
                .add(IndexPrivilege.READ, "index")
                .build();
        final String action = IndicesAliasesAction.NAME;
        IndicesAccessControl accessControl = new IndicesAccessControl(true, Collections.emptyMap());
        threadContext.putTransient(AuthorizationServiceField.INDICES_PERMISSIONS_KEY, accessControl);
        IndicesAliasesRequestInterceptor interceptor =
                new IndicesAliasesRequestInterceptor(threadContext, licenseState, auditTrailService);

        final IndicesAliasesRequest indicesAliasesRequest = new IndicesAliasesRequest();
        if (randomBoolean()) {
            indicesAliasesRequest.addAliasAction(IndicesAliasesRequest.AliasActions.remove().index("bar").alias(randomAlphaOfLength(4)));
        }
        indicesAliasesRequest.addAliasAction(IndicesAliasesRequest.AliasActions.add().index("index").alias("alias"));
        if (randomBoolean()) {
            indicesAliasesRequest.addAliasAction(IndicesAliasesRequest.AliasActions.removeIndex().index("foofoo"));
        }

        ElasticsearchSecurityException securityException = expectThrows(ElasticsearchSecurityException.class,
                () -> interceptor.intercept(indicesAliasesRequest, authentication, role, action));
        assertEquals("Adding an alias is not allowed when the alias has more permissions than any of the indices",
                securityException.getMessage());

        // swap target and source for success
        final IndicesAliasesRequest successRequest = new IndicesAliasesRequest();
        if (randomBoolean()) {
            successRequest.addAliasAction(IndicesAliasesRequest.AliasActions.remove().index("bar").alias(randomAlphaOfLength(4)));
        }
        successRequest.addAliasAction(IndicesAliasesRequest.AliasActions.add().index("alias").alias("index"));
        if (randomBoolean()) {
            successRequest.addAliasAction(IndicesAliasesRequest.AliasActions.removeIndex().index("foofoo"));
        }
        interceptor.intercept(successRequest, authentication, role, action);
    }
}

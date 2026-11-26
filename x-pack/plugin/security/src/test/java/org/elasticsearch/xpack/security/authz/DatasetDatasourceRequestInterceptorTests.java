/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authz;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.license.MockLicenseState;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.esql.EsqlDatasetActionNames;
import org.elasticsearch.xpack.core.security.authc.AuthenticationTestHelper;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine.RequestInfo;
import org.elasticsearch.xpack.core.security.authz.RestrictedIndices;
import org.elasticsearch.xpack.core.security.authz.permission.Role;
import org.elasticsearch.xpack.core.security.authz.privilege.ConfigurableClusterPrivileges;
import org.elasticsearch.xpack.core.security.authz.privilege.ConfigurableClusterPrivileges.DatasourcePrivileges.DatasourcePermissionGroup;
import org.elasticsearch.xpack.core.security.support.Automatons;
import org.elasticsearch.xpack.security.Security;
import org.elasticsearch.xpack.security.action.MockDataSourceRequest;
import org.elasticsearch.xpack.security.audit.AuditTrail;
import org.elasticsearch.xpack.security.audit.AuditTrailService;
import org.elasticsearch.xpack.security.audit.AuditUtil;
import org.elasticsearch.xpack.security.authz.RBACEngine.RBACAuthorizationInfo;
import org.elasticsearch.xpack.security.authz.interceptor.DatasetDatasourceRequestInterceptor;
import org.junit.Before;

import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class DatasetDatasourceRequestInterceptorTests extends ESTestCase {

    private DatasetDatasourceRequestInterceptor interceptor;
    private AuditTrail auditTrail;
    private String requestId;

    @Before
    public void setUpInterceptor() {
        var licenseState = mock(MockLicenseState.class);
        when(licenseState.copyCurrentLicenseState()).thenReturn(licenseState);
        when(licenseState.isAllowed(Security.AUDITING_FEATURE)).thenReturn(true);

        var settings = Settings.builder().put(XPackSettings.AUDIT_ENABLED.getKey(), true).build();

        auditTrail = mock(AuditTrail.class);
        var threadContext = new ThreadContext(Settings.EMPTY);
        var clusterService = mock(ClusterService.class);
        when(clusterService.getClusterSettings()).thenReturn(new ClusterSettings(settings, Set.of(XPackSettings.AUDIT_ENABLED)));
        var auditTrailService = new AuditTrailService(auditTrail, licenseState, clusterService);

        requestId = AuditUtil.getOrGenerateRequestId(threadContext);
        interceptor = new DatasetDatasourceRequestInterceptor(threadContext, auditTrailService);
    }

    public void testPutDatasetDatasourceAuthorized() throws Exception {
        Role role = roleWithDatasourceRead("myds");
        RequestInfo requestInfo = putDatasetRequestInfo("myds");
        var future = new PlainActionFuture<Void>();
        interceptor.intercept(requestInfo, mock(AuthorizationEngine.class), rbacInfo(role)).addListener(future);
        assertNull(future.actionGet());
    }

    public void testPutDatasetDatasourceDenied() {
        Role role = roleWithDatasourceRead("myds");
        RequestInfo requestInfo = putDatasetRequestInfo("other");
        var future = new PlainActionFuture<Void>();
        interceptor.intercept(requestInfo, mock(AuthorizationEngine.class), rbacInfo(role)).addListener(future);
        ElasticsearchSecurityException e = expectThrows(ElasticsearchSecurityException.class, future::actionGet);
        assertThat(e.getMessage(), containsString(EsqlDatasetActionNames.ESQL_AUTHORIZE_DATASET_DATASOURCE_ACTION_NAME));

        verify(auditTrail).accessDenied(
            requestId,
            requestInfo.getAuthentication(),
            requestInfo.getAction(),
            requestInfo.getRequest(),
            rbacInfo(role)
        );
        verifyNoMoreInteractions(auditTrail);
    }

    public void testSkipsWhenDatasourceActionMatchesRequestAction() {
        Role role = roleWithDatasourceRead("myds");
        RequestInfo requestInfo = new RequestInfo(
            AuthenticationTestHelper.builder().build(),
            new MockDataSourceRequest(EsqlDatasetActionNames.ESQL_PUT_DATASET_ACTION_NAME, "myds"),
            EsqlDatasetActionNames.ESQL_PUT_DATASET_ACTION_NAME,
            null
        );
        var future = new PlainActionFuture<Void>();
        interceptor.intercept(requestInfo, mock(AuthorizationEngine.class), rbacInfo(role)).addListener(future);
        assertNull(future.actionGet());
    }

    private static RequestInfo putDatasetRequestInfo(String dataSource) {
        return new RequestInfo(
            AuthenticationTestHelper.builder().build(),
            new MockDataSourceRequest(EsqlDatasetActionNames.ESQL_AUTHORIZE_DATASET_DATASOURCE_ACTION_NAME, dataSource),
            EsqlDatasetActionNames.ESQL_PUT_DATASET_ACTION_NAME,
            null
        );
    }

    private static RBACAuthorizationInfo rbacInfo(Role role) {
        return new RBACAuthorizationInfo(role, role);
    }

    private static Role roleWithDatasourceRead(String name) {
        ConfigurableClusterPrivileges.DatasourcePrivileges privilege = new ConfigurableClusterPrivileges.DatasourcePrivileges(
            List.of(new DatasourcePermissionGroup(new String[] { name }, new String[] { "read" }))
        );
        return Role.builder(new RestrictedIndices(Automatons.EMPTY)).cluster(Set.of(), List.of(privilege)).build();
    }
}

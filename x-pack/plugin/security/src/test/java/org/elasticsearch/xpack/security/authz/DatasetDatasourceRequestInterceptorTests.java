/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authz;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.esql.DataSourceRequestInfo;
import org.elasticsearch.xpack.core.esql.EsqlDatasetActionNames;
import org.elasticsearch.xpack.core.security.authc.AuthenticationTestHelper;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine.RequestInfo;
import org.elasticsearch.xpack.core.security.authz.RestrictedIndices;
import org.elasticsearch.xpack.core.security.authz.permission.Role;
import org.elasticsearch.xpack.core.security.authz.privilege.ConfigurableClusterPrivileges;
import org.elasticsearch.xpack.core.security.authz.privilege.ConfigurableClusterPrivileges.ManageDatasourcePrivileges.DatasourcePermissionGroup;
import org.elasticsearch.xpack.core.security.support.Automatons;
import org.elasticsearch.xpack.security.authz.RBACEngine.RBACAuthorizationInfo;
import org.elasticsearch.xpack.security.authz.interceptor.DatasetDatasourceRequestInterceptor;

import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;
import static org.mockito.Mockito.mock;

public class DatasetDatasourceRequestInterceptorTests extends ESTestCase {

    private final DatasetDatasourceRequestInterceptor interceptor = new DatasetDatasourceRequestInterceptor();

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
        assertThat(
            e.getMessage(),
            containsString(EsqlDatasetActionNames.ESQL_AUTHORIZE_DATASET_DATASOURCE_ACTION_NAME)
        );
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
        ConfigurableClusterPrivileges.ManageDatasourcePrivileges privilege = new ConfigurableClusterPrivileges.ManageDatasourcePrivileges(
            List.of(new DatasourcePermissionGroup(new String[] { name }, new String[] { "read" }))
        );
        return Role.builder(new RestrictedIndices(Automatons.EMPTY)).cluster(Set.of(), List.of(privilege)).build();
    }

    private static final class MockDataSourceRequest extends ActionRequest implements DataSourceRequestInfo {
        private final String actionName;
        private final String[] names;

        MockDataSourceRequest(String actionName, String... names) {
            this.actionName = actionName;
            this.names = names;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public String[] dataSourceNames() {
            return names;
        }

        @Override
        public String dataSourceClusterActionName() {
            return actionName;
        }
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authz.privilege;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xpack.core.esql.DataSourceRequestInfo;
import org.elasticsearch.xpack.core.esql.EsqlDataSourceActionNames;
import org.elasticsearch.xpack.core.esql.EsqlDatasetActionNames;
import org.elasticsearch.xpack.core.security.authc.AuthenticationTestHelper;
import org.elasticsearch.xpack.core.security.authz.permission.ClusterPermission;
import org.elasticsearch.xpack.core.security.authz.privilege.ConfigurableClusterPrivileges.ManageDatasourcePrivileges.DatasourcePermissionGroup;

import java.io.IOException;
import java.util.List;

import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.is;

public class ManageDatasourcePrivilegesTests extends ESTestCase {

    public void testRoundTripNamedWriteable() throws IOException {
        ConfigurableClusterPrivileges.ManageDatasourcePrivileges original = new ConfigurableClusterPrivileges.ManageDatasourcePrivileges(
            List.of(new DatasourcePermissionGroup(new String[] { "ds-*" }, new String[] { "manage" }))
        );
        NamedWriteableRegistry registry = new NamedWriteableRegistry(
            singletonList(
                new NamedWriteableRegistry.Entry(
                    ConfigurableClusterPrivilege.class,
                    ConfigurableClusterPrivileges.ManageDatasourcePrivileges.WRITEABLE_NAME,
                    ConfigurableClusterPrivileges.ManageDatasourcePrivileges::createFrom
                )
            )
        );
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            original.writeTo(out);
            try (StreamInput in = new NamedWriteableAwareStreamInput(out.bytes().streamInput(), registry)) {
                ConfigurableClusterPrivileges.ManageDatasourcePrivileges read = ConfigurableClusterPrivileges.ManageDatasourcePrivileges
                    .createFrom(in);
                assertEquals(original, read);
            }
        }
    }

    public void testClusterPermissionAllowsManage() {
        ConfigurableClusterPrivileges.ManageDatasourcePrivileges privilege = new ConfigurableClusterPrivileges.ManageDatasourcePrivileges(
            List.of(new DatasourcePermissionGroup(new String[] { "abc*" }, new String[] { "manage" }))
        );
        ClusterPermission permission = privilege.buildPermission(ClusterPermission.builder()).build();
        TransportRequest put = new MockDataSourceRequest(EsqlDataSourceActionNames.ESQL_PUT_DATA_SOURCE_ACTION_NAME, "abc1");
        assertThat(
            permission.check(EsqlDataSourceActionNames.ESQL_PUT_DATA_SOURCE_ACTION_NAME, put, AuthenticationTestHelper.builder().build()),
            is(true)
        );
        TransportRequest putDenied = new MockDataSourceRequest(EsqlDataSourceActionNames.ESQL_PUT_DATA_SOURCE_ACTION_NAME, "zzz");
        assertThat(
            permission.check(
                EsqlDataSourceActionNames.ESQL_PUT_DATA_SOURCE_ACTION_NAME,
                putDenied,
                AuthenticationTestHelper.builder().build()
            ),
            is(false)
        );
    }

    /**
     * A single {@code *} name pattern matches every datasource; {@code read} covers metadata reads, dataset
     * datasource authorization, but not create/delete without {@code manage} or those privileges explicitly.
     */
    public void testWildcardNameMatchesAllDatasources() {
        ConfigurableClusterPrivileges.ManageDatasourcePrivileges privilege = new ConfigurableClusterPrivileges.ManageDatasourcePrivileges(
            List.of(new DatasourcePermissionGroup(new String[] { "*" }, new String[] { "read" }))
        );
        ClusterPermission permission = privilege.buildPermission(ClusterPermission.builder()).build();
        var auth = AuthenticationTestHelper.builder().build();
        String arbitrary = "any_ds_" + randomAlphaOfLength(8);
        assertThat(
            permission.check(
                EsqlDatasetActionNames.ESQL_AUTHORIZE_DATASET_DATASOURCE_ACTION_NAME,
                new MockDataSourceRequest(EsqlDatasetActionNames.ESQL_AUTHORIZE_DATASET_DATASOURCE_ACTION_NAME, arbitrary),
                auth
            ),
            is(true)
        );
        assertThat(
            permission.check(
                EsqlDataSourceActionNames.ESQL_GET_DATA_SOURCE_ACTION_NAME,
                new MockDataSourceRequest(EsqlDataSourceActionNames.ESQL_GET_DATA_SOURCE_ACTION_NAME, arbitrary),
                auth
            ),
            is(true)
        );
        assertThat(
            permission.check(
                EsqlDataSourceActionNames.ESQL_PUT_DATA_SOURCE_ACTION_NAME,
                new MockDataSourceRequest(EsqlDataSourceActionNames.ESQL_PUT_DATA_SOURCE_ACTION_NAME, arbitrary),
                auth
            ),
            is(false)
        );
        assertThat(
            permission.check(
                EsqlDataSourceActionNames.ESQL_DELETE_DATA_SOURCE_ACTION_NAME,
                new MockDataSourceRequest(EsqlDataSourceActionNames.ESQL_DELETE_DATA_SOURCE_ACTION_NAME, arbitrary),
                auth
            ),
            is(false)
        );
    }

    public void testAuthorizeDatasetDatasourceRequiresRead() {
        ConfigurableClusterPrivileges.ManageDatasourcePrivileges privilege = new ConfigurableClusterPrivileges.ManageDatasourcePrivileges(
            List.of(new DatasourcePermissionGroup(new String[] { "mys3" }, new String[] { "read" }))
        );
        ClusterPermission permission = privilege.buildPermission(ClusterPermission.builder()).build();
        TransportRequest authorize = new MockDataSourceRequest(
            EsqlDatasetActionNames.ESQL_AUTHORIZE_DATASET_DATASOURCE_ACTION_NAME,
            "mys3"
        );
        var auth = AuthenticationTestHelper.builder().build();
        assertThat(permission.check(EsqlDatasetActionNames.ESQL_AUTHORIZE_DATASET_DATASOURCE_ACTION_NAME, authorize, auth), is(true));
        TransportRequest denied = new MockDataSourceRequest(EsqlDatasetActionNames.ESQL_AUTHORIZE_DATASET_DATASOURCE_ACTION_NAME, "other");
        assertThat(permission.check(EsqlDatasetActionNames.ESQL_AUTHORIZE_DATASET_DATASOURCE_ACTION_NAME, denied, auth), is(false));
    }

    public void testClusterPermissionCreateVsRead() {
        ConfigurableClusterPrivileges.ManageDatasourcePrivileges privilege = new ConfigurableClusterPrivileges.ManageDatasourcePrivileges(
            List.of(new DatasourcePermissionGroup(new String[] { "myds" }, new String[] { "create", "read_metadata" }))
        );
        ClusterPermission permission = privilege.buildPermission(ClusterPermission.builder()).build();
        TransportRequest put = new MockDataSourceRequest(EsqlDataSourceActionNames.ESQL_PUT_DATA_SOURCE_ACTION_NAME, "myds");
        TransportRequest get = new MockDataSourceRequest(EsqlDataSourceActionNames.ESQL_GET_DATA_SOURCE_ACTION_NAME, "myds");
        var auth = AuthenticationTestHelper.builder().build();
        assertThat(permission.check(EsqlDataSourceActionNames.ESQL_PUT_DATA_SOURCE_ACTION_NAME, put, auth), is(true));
        assertThat(permission.check(EsqlDataSourceActionNames.ESQL_GET_DATA_SOURCE_ACTION_NAME, get, auth), is(true));
        TransportRequest delete = new MockDataSourceRequest(EsqlDataSourceActionNames.ESQL_DELETE_DATA_SOURCE_ACTION_NAME, "myds");
        assertThat(permission.check(EsqlDataSourceActionNames.ESQL_DELETE_DATA_SOURCE_ACTION_NAME, delete, auth), is(false));
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

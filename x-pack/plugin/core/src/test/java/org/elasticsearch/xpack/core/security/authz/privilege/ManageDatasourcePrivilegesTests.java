/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authz.privilege;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.test.AbstractNamedWriteableTestCase;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xpack.core.XPackClientPlugin;
import org.elasticsearch.xpack.core.esql.DataSourceRequestInfo;
import org.elasticsearch.xpack.core.esql.EsqlDataSourceActionNames;
import org.elasticsearch.xpack.core.esql.EsqlDatasetActionNames;
import org.elasticsearch.xpack.core.security.authc.AuthenticationTestHelper;
import org.elasticsearch.xpack.core.security.authz.permission.ClusterPermission;
import org.elasticsearch.xpack.core.security.authz.privilege.ConfigurableClusterPrivileges.ManageDatasourcePrivileges;
import org.elasticsearch.xpack.core.security.authz.privilege.ConfigurableClusterPrivileges.ManageDatasourcePrivileges.DatasourcePermissionGroup;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.is;

public class ManageDatasourcePrivilegesTests extends AbstractNamedWriteableTestCase<ConfigurableClusterPrivilege> {

    public void testClusterPermissionAllowsManage() {
        ManageDatasourcePrivileges privilege = new ManageDatasourcePrivileges(
            List.of(new DatasourcePermissionGroup(new String[] { "abc*" }, new String[] { ManageDatasourcePrivileges.PRIVILEGE_MANAGE }))
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
        ManageDatasourcePrivileges privilege = new ManageDatasourcePrivileges(
            List.of(new DatasourcePermissionGroup(new String[] { "*" }, new String[] { ManageDatasourcePrivileges.PRIVILEGE_READ }))
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
        ManageDatasourcePrivileges privilege = new ManageDatasourcePrivileges(
            List.of(new DatasourcePermissionGroup(new String[] { "mys3" }, new String[] { ManageDatasourcePrivileges.PRIVILEGE_READ }))
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

    public void testConstructorRejectsUnsupportedPrivilege() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new ManageDatasourcePrivileges(
                List.of(new DatasourcePermissionGroup(new String[] { "myds" }, new String[] { "not_a_privilege" }))
            )
        );
        assertThat(e.getMessage(), is("unsupported datasource privilege [not_a_privilege]"));
    }

    public void testClusterPermissionCreateVsRead() {
        ManageDatasourcePrivileges privilege = new ManageDatasourcePrivileges(
            List.of(
                new DatasourcePermissionGroup(
                    new String[] { "myds" },
                    new String[] { ManageDatasourcePrivileges.PRIVILEGE_CREATE, ManageDatasourcePrivileges.PRIVILEGE_READ_METADATA }
                )
            )
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

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        try (var xClientPlugin = new XPackClientPlugin()) {
            return new NamedWriteableRegistry(xClientPlugin.getNamedWriteables());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected Class<ConfigurableClusterPrivilege> categoryClass() {
        return ConfigurableClusterPrivilege.class;
    }

    @Override
    protected ConfigurableClusterPrivilege createTestInstance() {
        return buildTestPrivileges();
    }

    @Override
    protected ConfigurableClusterPrivilege mutateInstance(ConfigurableClusterPrivilege instance) throws IOException {
        if (instance instanceof ManageDatasourcePrivileges == false) {
            fail("not a ManageDatasourcePrivileges");
        }
        ManageDatasourcePrivileges mutated = buildTestPrivileges();
        if (mutated.equals(instance)) {
            return new ManageDatasourcePrivileges(
                List.of(
                    new DatasourcePermissionGroup(
                        new String[] { "mutated-" + randomAlphaOfLength(4) },
                        new String[] { ManageDatasourcePrivileges.PRIVILEGE_READ }
                    )
                )
            );
        }
        return mutated;
    }

    private static ManageDatasourcePrivileges buildTestPrivileges() {
        int groupCount = randomIntBetween(1, 3);
        List<DatasourcePermissionGroup> groups = new ArrayList<>(groupCount);
        List<String> allowedPrivileges = List.of(
            ManageDatasourcePrivileges.PRIVILEGE_CREATE,
            ManageDatasourcePrivileges.PRIVILEGE_DELETE,
            ManageDatasourcePrivileges.PRIVILEGE_READ_METADATA,
            ManageDatasourcePrivileges.PRIVILEGE_READ,
            ManageDatasourcePrivileges.PRIVILEGE_MANAGE
        );
        for (int i = 0; i < groupCount; i++) {
            groups.add(
                new DatasourcePermissionGroup(
                    generateRandomStringArray(randomIntBetween(1, 3), randomIntBetween(2, 10), false, false),
                    randomSubsetOf(randomIntBetween(1, allowedPrivileges.size()), allowedPrivileges).toArray(String[]::new)
                )
            );
        }
        return new ManageDatasourcePrivileges(groups);
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

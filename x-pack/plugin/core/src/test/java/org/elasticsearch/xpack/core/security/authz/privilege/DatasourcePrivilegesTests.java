/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authz.privilege;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.test.AbstractNamedWriteableTestCase;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.XPackClientPlugin;
import org.elasticsearch.xpack.core.esql.DataSourceRequestInfo;
import org.elasticsearch.xpack.core.esql.EsqlDataSourceActionNames;
import org.elasticsearch.xpack.core.esql.EsqlDatasetActionNames;
import org.elasticsearch.xpack.core.security.authc.AuthenticationTestHelper;
import org.elasticsearch.xpack.core.security.authz.permission.ClusterPermission;
import org.elasticsearch.xpack.core.security.authz.privilege.ConfigurableClusterPrivileges.DatasourcePrivileges;
import org.elasticsearch.xpack.core.security.authz.privilege.ConfigurableClusterPrivileges.DatasourcePrivileges.DatasourcePermissionGroup;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

public class DatasourcePrivilegesTests extends AbstractNamedWriteableTestCase<ConfigurableClusterPrivilege> {

    public void testClusterPermissionAllowsManage() {
        DatasourcePrivileges privilege = new DatasourcePrivileges(
            List.of(new DatasourcePermissionGroup(new String[] { "abc*" }, new String[] { DatasourcePrivileges.PRIVILEGE_MANAGE }))
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
     * A single {@code *} name pattern matches every datasource; {@code read} covers dataset datasource authorization
     * but not GET metadata or create/delete without {@code read_metadata}, {@code manage}, or those privileges explicitly.
     */
    public void testWildcardNameMatchesAllDatasources() {
        DatasourcePrivileges privilege = new DatasourcePrivileges(
            List.of(new DatasourcePermissionGroup(new String[] { "*" }, new String[] { DatasourcePrivileges.PRIVILEGE_READ }))
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
            is(false)
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

    public void testGetRequiresReadMetadataNotRead() {
        DatasourcePrivileges privilege = new DatasourcePrivileges(
            List.of(new DatasourcePermissionGroup(new String[] { "myds" }, new String[] { DatasourcePrivileges.PRIVILEGE_READ }))
        );
        ClusterPermission permission = privilege.buildPermission(ClusterPermission.builder()).build();
        var auth = AuthenticationTestHelper.builder().build();
        TransportRequest get = new MockDataSourceRequest(EsqlDataSourceActionNames.ESQL_GET_DATA_SOURCE_ACTION_NAME, "myds");
        assertThat(permission.check(EsqlDataSourceActionNames.ESQL_GET_DATA_SOURCE_ACTION_NAME, get, auth), is(false));
        DatasourcePrivileges withMetadata = new DatasourcePrivileges(
            List.of(new DatasourcePermissionGroup(new String[] { "myds" }, new String[] { DatasourcePrivileges.PRIVILEGE_READ_METADATA }))
        );
        ClusterPermission metadataPermission = withMetadata.buildPermission(ClusterPermission.builder()).build();
        assertThat(metadataPermission.check(EsqlDataSourceActionNames.ESQL_GET_DATA_SOURCE_ACTION_NAME, get, auth), is(true));
    }

    public void testAuthorizeDatasetDatasourceRequiresRead() {
        DatasourcePrivileges privilege = new DatasourcePrivileges(
            List.of(new DatasourcePermissionGroup(new String[] { "mys3" }, new String[] { DatasourcePrivileges.PRIVILEGE_READ }))
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
            () -> new DatasourcePrivileges(
                List.of(new DatasourcePermissionGroup(new String[] { "myds" }, new String[] { "not_a_privilege" }))
            )
        );
        assertThat(e.getMessage(), is("unsupported datasource privilege [not_a_privilege]"));
    }

    public void testClusterPermissionCreateVsRead() {
        DatasourcePrivileges privilege = new DatasourcePrivileges(
            List.of(
                new DatasourcePermissionGroup(
                    new String[] { "myds" },
                    new String[] { DatasourcePrivileges.PRIVILEGE_CREATE, DatasourcePrivileges.PRIVILEGE_READ_METADATA }
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

    /**
     * {@link ConfigurableClusterPrivileges#toXContent} opens the {@code global.data_source} array; privilege serialization must write
     * group objects only ({@link DatasourcePrivileges#toXContentArrayElements}), not a nested array.
     */
    public void testGlobalToXContentDataSourceIsSingleArray() throws IOException {
        DatasourcePrivileges privilege = sampleDatasourcePrivileges();
        XContentBuilder builder = XContentBuilder.builder(JsonXContent.jsonXContent);
        ConfigurableClusterPrivileges.toXContent(builder, ToXContent.EMPTY_PARAMS, List.of(privilege));
        builder.close();

        String json = BytesReference.bytes(builder).utf8ToString();
        assertThat(json, containsString("\"data_source\":[{"));
        assertThat(json, not(containsString("\"data_source\":[[{")));

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(builder))) {
            assertThat(parser.nextToken(), equalTo(XContentParser.Token.START_OBJECT));
            boolean foundDataSource = false;
            XContentParser.Token token;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                assertThat(token, equalTo(XContentParser.Token.FIELD_NAME));
                if ("data_source".equals(parser.currentName())) {
                    foundDataSource = true;
                    assertThat(parser.nextToken(), equalTo(XContentParser.Token.START_ARRAY));
                    assertThat(parser.nextToken(), equalTo(XContentParser.Token.START_OBJECT));
                    parser.skipChildren();
                    assertThat(parser.nextToken(), equalTo(XContentParser.Token.START_OBJECT));
                    parser.skipChildren();
                    assertThat(parser.nextToken(), equalTo(XContentParser.Token.END_ARRAY));
                } else {
                    assertThat(parser.nextToken(), equalTo(XContentParser.Token.START_OBJECT));
                    parser.skipChildren();
                }
            }
            assertTrue(foundDataSource);
        }
    }

    public void testParseRolesYamlShapeAndRoundTripToXContent() throws IOException {
        String global = """
            {
              "application": {},
              "profile": {},
              "role": {},
              "data_source": [
                {
                  "names": [ "security_it_*" ],
                  "privileges": [ "read_metadata" ]
                },
                {
                  "names": [ "other_*" ],
                  "privileges": [ "read" ]
                }
              ]
            }
            """;
        DatasourcePrivileges expected = new DatasourcePrivileges(
            List.of(
                new DatasourcePermissionGroup(new String[] { "security_it_*" }, new String[] { "read_metadata" }),
                new DatasourcePermissionGroup(new String[] { "other_*" }, new String[] { "read" })
            )
        );

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, global)) {
            assertThat(parser.nextToken(), equalTo(XContentParser.Token.START_OBJECT));
            List<ConfigurableClusterPrivilege> parsed = ConfigurableClusterPrivileges.parse(parser);
            assertThat(parsed.size(), equalTo(1));
            assertThat(parsed.get(0), equalTo(expected));
        }

        XContentBuilder builder = XContentBuilder.builder(JsonXContent.jsonXContent);
        ConfigurableClusterPrivileges.toXContent(builder, ToXContent.EMPTY_PARAMS, List.of(expected));
        builder.close();

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(builder))) {
            assertThat(parser.nextToken(), equalTo(XContentParser.Token.START_OBJECT));
            List<ConfigurableClusterPrivilege> roundTripped = ConfigurableClusterPrivileges.parse(parser);
            assertThat(roundTripped.size(), equalTo(1));
            assertThat(roundTripped.get(0), equalTo(expected));
        }
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
        if (instance instanceof DatasourcePrivileges == false) {
            fail("not a DatasourcePrivileges");
        }
        DatasourcePrivileges mutated = buildTestPrivileges();
        if (mutated.equals(instance)) {
            return new DatasourcePrivileges(
                List.of(
                    new DatasourcePermissionGroup(
                        new String[] { "mutated-" + randomAlphaOfLength(4) },
                        new String[] { DatasourcePrivileges.PRIVILEGE_READ }
                    )
                )
            );
        }
        return mutated;
    }

    private static DatasourcePrivileges sampleDatasourcePrivileges() {
        return new DatasourcePrivileges(
            List.of(
                new DatasourcePermissionGroup(
                    new String[] { "security_it_*" },
                    new String[] { DatasourcePrivileges.PRIVILEGE_READ_METADATA }
                ),
                new DatasourcePermissionGroup(new String[] { "other_*" }, new String[] { DatasourcePrivileges.PRIVILEGE_READ })
            )
        );
    }

    private static DatasourcePrivileges buildTestPrivileges() {
        int groupCount = randomIntBetween(1, 3);
        List<DatasourcePermissionGroup> groups = new ArrayList<>(groupCount);
        List<String> allowedPrivileges = List.of(
            DatasourcePrivileges.PRIVILEGE_CREATE,
            DatasourcePrivileges.PRIVILEGE_DELETE,
            DatasourcePrivileges.PRIVILEGE_READ_METADATA,
            DatasourcePrivileges.PRIVILEGE_READ,
            DatasourcePrivileges.PRIVILEGE_MANAGE
        );
        for (int i = 0; i < groupCount; i++) {
            groups.add(
                new DatasourcePermissionGroup(
                    generateRandomStringArray(randomIntBetween(1, 3), randomIntBetween(2, 10), false, false),
                    randomSubsetOf(randomIntBetween(1, allowedPrivileges.size()), allowedPrivileges).toArray(String[]::new)
                )
            );
        }
        return new DatasourcePrivileges(groups);
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

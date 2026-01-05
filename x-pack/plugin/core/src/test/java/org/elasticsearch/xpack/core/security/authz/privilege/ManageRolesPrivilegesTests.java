/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authz.privilege;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.test.AbstractNamedWriteableTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.XPackClientPlugin;
import org.elasticsearch.xpack.core.security.action.role.BulkDeleteRolesRequest;
import org.elasticsearch.xpack.core.security.action.role.BulkPutRolesRequest;
import org.elasticsearch.xpack.core.security.action.role.DeleteRoleRequest;
import org.elasticsearch.xpack.core.security.action.role.PutRoleRequest;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationTestHelper;
import org.elasticsearch.xpack.core.security.authz.RestrictedIndices;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.permission.ClusterPermission;
import org.elasticsearch.xpack.core.security.authz.permission.RemoteClusterPermissionGroup;
import org.elasticsearch.xpack.core.security.authz.permission.RemoteClusterPermissions;
import org.elasticsearch.xpack.core.security.authz.privilege.ConfigurableClusterPrivileges.ManageRolesPrivilege;
import org.elasticsearch.xpack.core.security.authz.store.ReservedRolesStore;
import org.elasticsearch.xpack.core.security.test.TestRestrictedIndices;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsEqual.equalTo;

public class ManageRolesPrivilegesTests extends AbstractNamedWriteableTestCase<ConfigurableClusterPrivilege> {

    private static final int MIN_INDEX_NAME_LENGTH = 4;

    public void testSimplePutRoleRequest() {
        new ReservedRolesStore();
        final ManageRolesPrivilege privilege = new ManageRolesPrivilege(
            List.of(new ManageRolesPrivilege.ManageRolesIndexPermissionGroup(new String[] { "allowed*" }, new String[] { "all" }))
        );
        final ClusterPermission permission = privilege.buildPermission(
            new ClusterPermission.Builder(new RestrictedIndices(TestRestrictedIndices.RESTRICTED_INDICES.getAutomaton()))
        ).build();

        assertAllowedIndexPatterns(permission, randomArray(1, 10, String[]::new, () -> "allowed-" + randomAlphaOfLength(5)), true);
        assertAllowedIndexPatterns(permission, randomArray(1, 10, String[]::new, () -> "not-allowed-" + randomAlphaOfLength(5)), false);
        assertAllowedIndexPatterns(
            permission,
            new String[] { "allowed-" + randomAlphaOfLength(5), "not-allowed-" + randomAlphaOfLength(5) },
            false
        );
    }

    public void testDeleteRoleRequest() {
        new ReservedRolesStore();
        {
            final ManageRolesPrivilege privilege = new ManageRolesPrivilege(
                List.of(new ManageRolesPrivilege.ManageRolesIndexPermissionGroup(new String[] { "allowed*" }, new String[] { "manage" }))
            );
            final ClusterPermission permission = privilege.buildPermission(
                new ClusterPermission.Builder(new RestrictedIndices(TestRestrictedIndices.RESTRICTED_INDICES.getAutomaton()))
            ).build();

            assertAllowedDeleteIndex(permission, randomArray(1, 10, String[]::new, () -> "allowed-" + randomAlphaOfLength(5)), true);
            assertAllowedDeleteIndex(permission, randomArray(1, 10, String[]::new, () -> "not-allowed-" + randomAlphaOfLength(5)), false);
            assertAllowedDeleteIndex(
                permission,
                new String[] { "allowed-" + randomAlphaOfLength(5), "not-allowed-" + randomAlphaOfLength(5) },
                false
            );
        }
        {
            final ManageRolesPrivilege privilege = new ManageRolesPrivilege(
                List.of(new ManageRolesPrivilege.ManageRolesIndexPermissionGroup(new String[] { "allowed*" }, new String[] { "read" }))
            );
            final ClusterPermission permission = privilege.buildPermission(
                new ClusterPermission.Builder(new RestrictedIndices(TestRestrictedIndices.RESTRICTED_INDICES.getAutomaton()))
            ).build();
            assertAllowedDeleteIndex(permission, randomArray(1, 10, String[]::new, () -> "allowed-" + randomAlphaOfLength(5)), false);
        }
    }

    public void testSeveralIndexGroupsPutRoleRequest() {
        new ReservedRolesStore();

        final ManageRolesPrivilege privilege = new ManageRolesPrivilege(
            List.of(
                new ManageRolesPrivilege.ManageRolesIndexPermissionGroup(new String[] { "a", "b" }, new String[] { "read" }),
                new ManageRolesPrivilege.ManageRolesIndexPermissionGroup(new String[] { "c" }, new String[] { "read" }),
                new ManageRolesPrivilege.ManageRolesIndexPermissionGroup(new String[] { "d" }, new String[] { "read" })
            )
        );

        final ClusterPermission permission = privilege.buildPermission(
            new ClusterPermission.Builder(new RestrictedIndices(TestRestrictedIndices.RESTRICTED_INDICES.getAutomaton()))
        ).build();

        assertAllowedIndexPatterns(permission, new String[] { "/[ab]/" }, new String[] { "read" }, true);
        assertAllowedIndexPatterns(permission, new String[] { "/[cd]/" }, new String[] { "read" }, true);
        assertAllowedIndexPatterns(permission, new String[] { "/[acd]/" }, new String[] { "read" }, true);
        assertAllowedIndexPatterns(permission, new String[] { "/[ef]/" }, new String[] { "read" }, false);
    }

    public void testPrivilegeIntersectionPutRoleRequest() {
        new ReservedRolesStore();

        final ManageRolesPrivilege privilege = new ManageRolesPrivilege(
            List.of(
                new ManageRolesPrivilege.ManageRolesIndexPermissionGroup(new String[] { "a", "b" }, new String[] { "all" }),
                new ManageRolesPrivilege.ManageRolesIndexPermissionGroup(new String[] { "c" }, new String[] { "create" }),
                new ManageRolesPrivilege.ManageRolesIndexPermissionGroup(new String[] { "d" }, new String[] { "delete" }),
                new ManageRolesPrivilege.ManageRolesIndexPermissionGroup(new String[] { "e" }, new String[] { "create_doc" }),
                new ManageRolesPrivilege.ManageRolesIndexPermissionGroup(new String[] { "f" }, new String[] { "read", "manage" })
            )
        );

        final ClusterPermission permission = privilege.buildPermission(
            new ClusterPermission.Builder(new RestrictedIndices(TestRestrictedIndices.RESTRICTED_INDICES.getAutomaton()))
        ).build();

        assertAllowedIndexPatterns(permission, new String[] { "/[ab]/" }, new String[] { "all" }, true);
        assertAllowedIndexPatterns(permission, new String[] { "/[abc]/" }, new String[] { "all" }, false);
        assertAllowedIndexPatterns(permission, new String[] { "/[ab]/" }, new String[] { "read", "manage" }, true);

        assertAllowedIndexPatterns(permission, new String[] { "/[ac]/" }, new String[] { "create" }, true);
        assertAllowedIndexPatterns(permission, new String[] { "/[ac]/" }, new String[] { "create", "create_doc" }, true);
        assertAllowedIndexPatterns(permission, new String[] { "/[ce]/" }, new String[] { "create_doc" }, true);
        assertAllowedIndexPatterns(permission, new String[] { "/[abce]/" }, new String[] { "create_doc" }, true);
        assertAllowedIndexPatterns(permission, new String[] { "/[abcde]/" }, new String[] { "create_doc" }, false);
        assertAllowedIndexPatterns(permission, new String[] { "/[ce]/" }, new String[] { "create_doc" }, true);
        assertAllowedIndexPatterns(permission, new String[] { "/[eb]/" }, new String[] { "create_doc" }, true);

        assertAllowedIndexPatterns(permission, new String[] { "/[d]/" }, new String[] { "delete" }, true);
        assertAllowedIndexPatterns(permission, new String[] { "/[ad]/" }, new String[] { "delete" }, true);
        assertAllowedIndexPatterns(permission, new String[] { "/[de]/" }, new String[] { "delete" }, false);

        assertAllowedIndexPatterns(permission, new String[] { "/[f]/" }, new String[] { "read", "manage" }, true);
        assertAllowedIndexPatterns(permission, new String[] { "/[f]/" }, new String[] { "read", "write" }, false);
        assertAllowedIndexPatterns(permission, new String[] { "/[f]/" }, new String[] { "read", "manage" }, true);
    }

    public void testEmptyPrivileges() {
        new ReservedRolesStore();

        final ManageRolesPrivilege privilege = new ManageRolesPrivilege(List.of());

        final ClusterPermission permission = privilege.buildPermission(
            new ClusterPermission.Builder(new RestrictedIndices(TestRestrictedIndices.RESTRICTED_INDICES.getAutomaton()))
        ).build();

        assertAllowedIndexPatterns(permission, new String[] { "test" }, new String[] { "all" }, false);
    }

    public void testRestrictedIndexPutRoleRequest() {
        new ReservedRolesStore();

        final ManageRolesPrivilege privilege = new ManageRolesPrivilege(
            List.of(new ManageRolesPrivilege.ManageRolesIndexPermissionGroup(new String[] { "*" }, new String[] { "all" }))
        );
        final ClusterPermission permission = privilege.buildPermission(
            new ClusterPermission.Builder(new RestrictedIndices(TestRestrictedIndices.RESTRICTED_INDICES.getAutomaton()))
        ).build();

        assertAllowedIndexPatterns(permission, new String[] { "security" }, true);
        assertAllowedIndexPatterns(permission, new String[] { ".security" }, false);
        assertAllowedIndexPatterns(permission, new String[] { "security", ".security-7" }, false);
    }

    public void testGenerateAndParseXContent() throws Exception {
        final XContent xContent = randomFrom(XContentType.values()).xContent();
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            final XContentBuilder builder = new XContentBuilder(xContent, out);

            final ManageRolesPrivilege original = buildPrivileges();
            builder.startObject();
            original.toXContent(builder, ToXContent.EMPTY_PARAMS);
            builder.endObject();
            builder.flush();

            final byte[] bytes = out.toByteArray();
            try (XContentParser parser = xContent.createParser(XContentParserConfiguration.EMPTY, bytes)) {
                assertThat(parser.nextToken(), equalTo(XContentParser.Token.START_OBJECT));
                assertThat(parser.nextToken(), equalTo(XContentParser.Token.FIELD_NAME));
                final ManageRolesPrivilege clone = ManageRolesPrivilege.parse(parser);
                assertThat(parser.nextToken(), equalTo(XContentParser.Token.END_OBJECT));

                assertThat(clone, equalTo(original));
                assertThat(original, equalTo(clone));
            }
        }
    }

    public void testPutRoleRequestContainsNonIndexPrivileges() {
        new ReservedRolesStore();
        final ManageRolesPrivilege privilege = new ManageRolesPrivilege(
            List.of(new ManageRolesPrivilege.ManageRolesIndexPermissionGroup(new String[] { "allowed*" }, new String[] { "all" }))
        );
        final ClusterPermission permission = privilege.buildPermission(
            new ClusterPermission.Builder(new RestrictedIndices(TestRestrictedIndices.RESTRICTED_INDICES.getAutomaton()))
        ).build();

        final PutRoleRequest putRoleRequest = new PutRoleRequest();

        switch (randomIntBetween(0, 5)) {
            case 0:
                putRoleRequest.cluster("all");
                break;
            case 1:
                putRoleRequest.runAs("test");
                break;
            case 2:
                putRoleRequest.addApplicationPrivileges(
                    RoleDescriptor.ApplicationResourcePrivileges.builder()
                        .privileges("all")
                        .application("test-app")
                        .resources("test-resource")
                        .build()
                );
                break;
            case 3:
                putRoleRequest.addRemoteIndex(
                    new RoleDescriptor.RemoteIndicesPrivileges.Builder("test-cluster").privileges("all").indices("test*").build()
                );
                break;
            case 4:
                putRoleRequest.putRemoteCluster(
                    new RemoteClusterPermissions().addGroup(
                        new RemoteClusterPermissionGroup(new String[] { "monitor_enrich" }, new String[] { "test" })
                    )
                );
                break;
            case 5:
                putRoleRequest.conditionalCluster(
                    new ConfigurableClusterPrivileges.ManageRolesPrivilege(
                        List.of(
                            new ManageRolesPrivilege.ManageRolesIndexPermissionGroup(new String[] { "test-*" }, new String[] { "read" })
                        )
                    )
                );
                break;
        }

        putRoleRequest.name(randomAlphaOfLength(4));
        assertThat(permissionCheck(permission, "cluster:admin/xpack/security/role/put", putRoleRequest), is(false));
    }

    public void testParseInvalidPrivilege() throws Exception {
        final String unknownPrivilege = randomValueOtherThanMany(
            i -> IndexPrivilege.values().containsKey(i),
            () -> randomAlphaOfLength(10).toLowerCase(Locale.ROOT)
        );

        final String invalidJsonString = String.format(Locale.ROOT, """
            {
                "manage": {
                    "indices": [
                        {
                            "names": ["test-*"],
                            "privileges": ["%s"]
                        }
                    ]
                }
            }""", unknownPrivilege);
        assertInvalidPrivilegeParsing(invalidJsonString, unknownPrivilege);
    }

    public void testParseMixedValidAndInvalidPrivileges() throws Exception {
        final String unknownPrivilege = randomValueOtherThanMany(
            i -> IndexPrivilege.values().containsKey(i),
            () -> randomAlphaOfLength(10).toLowerCase(Locale.ROOT)
        );

        final String validPrivilege = "read";
        final String mixedPrivilegesJson = String.format(Locale.ROOT, """
            {
                "manage": {
                    "indices": [
                        {
                            "names": ["test-*"],
                            "privileges": ["%s", "%s"]
                        }
                    ]
                }
            }""", validPrivilege, unknownPrivilege);

        assertInvalidPrivilegeParsing(mixedPrivilegesJson, unknownPrivilege);
    }

    /**
     * Helper method to assert that parsing the given JSON payload results in an
     * IllegalArgumentException due to an unknown privilege.
     *
     * @param jsonPayload The JSON string containing the privilege data.
     * @param expectedErrorDetail The specific unknown privilege name expected in the error message.
     */
    private static void assertInvalidPrivilegeParsing(final String jsonPayload, final String expectedErrorDetail) throws Exception {
        final XContent xContent = XContentType.JSON.xContent();

        try (
            XContentParser parser = xContent.createParser(XContentParserConfiguration.EMPTY, jsonPayload.getBytes(StandardCharsets.UTF_8))
        ) {
            assertThat(parser.nextToken(), equalTo(XContentParser.Token.START_OBJECT));
            assertThat(parser.nextToken(), equalTo(XContentParser.Token.FIELD_NAME));

            IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> ManageRolesPrivilege.parse(parser));

            assertThat(exception.getMessage(), containsString("unknown index privilege [" + expectedErrorDetail + "]"));
        }
    }

    private static boolean permissionCheck(ClusterPermission permission, String action, ActionRequest request) {
        final Authentication authentication = AuthenticationTestHelper.builder().build();
        assertThat(request.validate(), nullValue());
        return permission.check(action, request, authentication);
    }

    private static void assertAllowedIndexPatterns(ClusterPermission permission, String[] indexPatterns, boolean expected) {
        assertAllowedIndexPatterns(permission, indexPatterns, new String[] { "index", "write", "indices:data/read" }, expected);
    }

    private static void assertAllowedIndexPatterns(
        ClusterPermission permission,
        String[] indexPatterns,
        String[] privileges,
        boolean expected
    ) {
        {
            final PutRoleRequest putRoleRequest = new PutRoleRequest();
            putRoleRequest.name(randomAlphaOfLength(3));
            putRoleRequest.addIndex(indexPatterns, privileges, null, null, null, false);
            assertThat(permissionCheck(permission, "cluster:admin/xpack/security/role/put", putRoleRequest), is(expected));
        }
        {
            final BulkPutRolesRequest bulkPutRolesRequest = new BulkPutRolesRequest(
                List.of(
                    new RoleDescriptor(
                        randomAlphaOfLength(3),
                        new String[] {},
                        new RoleDescriptor.IndicesPrivileges[] {
                            RoleDescriptor.IndicesPrivileges.builder().indices(indexPatterns).privileges(privileges).build() },
                        new String[] {}
                    )
                )
            );
            assertThat(permissionCheck(permission, "cluster:admin/xpack/security/role/bulk_put", bulkPutRolesRequest), is(expected));
        }
    }

    private static void assertAllowedDeleteIndex(ClusterPermission permission, String[] indices, boolean expected) {
        {
            final BulkDeleteRolesRequest bulkDeleteRolesRequest = new BulkDeleteRolesRequest(List.of(indices));
            assertThat(permissionCheck(permission, "cluster:admin/xpack/security/role/bulk_delete", bulkDeleteRolesRequest), is(expected));
        }
        {
            assertThat(Arrays.stream(indices).allMatch(pattern -> {
                final DeleteRoleRequest deleteRolesRequest = new DeleteRoleRequest();
                deleteRolesRequest.name(pattern);
                return permissionCheck(permission, "cluster:admin/xpack/security/role/delete", deleteRolesRequest);
            }), is(expected));
        }
    }

    public static ManageRolesPrivilege buildPrivileges() {
        return buildPrivileges(randomIntBetween(MIN_INDEX_NAME_LENGTH, 7));
    }

    private static ManageRolesPrivilege buildPrivileges(int indexNameLength) {
        String[] indexNames = Objects.requireNonNull(generateRandomStringArray(5, indexNameLength, false, false));

        return new ManageRolesPrivilege(
            List.of(new ManageRolesPrivilege.ManageRolesIndexPermissionGroup(indexNames, IndexPrivilege.READ.name().toArray(String[]::new)))
        );
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
        return buildPrivileges();
    }

    @Override
    protected ConfigurableClusterPrivilege mutateInstance(ConfigurableClusterPrivilege instance) throws IOException {
        if (instance instanceof ManageRolesPrivilege) {
            return buildPrivileges(MIN_INDEX_NAME_LENGTH - 1);
        }
        fail();
        return null;
    }
}

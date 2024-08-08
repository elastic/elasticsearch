/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authz.privilege;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.util.set.Sets;
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
import org.elasticsearch.xpack.core.security.authz.privilege.ConfigurableClusterPrivileges.ManageRolesPrivilege;
import org.elasticsearch.xpack.core.security.authz.store.ReservedRolesStore;
import org.elasticsearch.xpack.core.security.test.TestRestrictedIndices;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsEqual.equalTo;

public class ManageRolesPrivilegesTests extends AbstractNamedWriteableTestCase<ConfigurableClusterPrivilege> {

    public void testSimplePutRoleRequest() {
        new ReservedRolesStore();
        final ManageRolesPrivilege privilege = new ManageRolesPrivilege(Sets.newHashSet("allowed-*"));
        final ClusterPermission permission = privilege.buildPermission(new ClusterPermission.Builder()).build();

        assertAllowedIndexPatterns(permission, randomArray(10, String[]::new, () -> "allowed-" + randomAlphaOfLength(5)), true);
        assertAllowedIndexPatterns(permission, randomArray(10, String[]::new, () -> "not-allowed-" + randomAlphaOfLength(5)), false);
        assertAllowedIndexPatterns(
            permission,
            new String[] { "allowed-" + randomAlphaOfLength(5), "not-allowed-" + randomAlphaOfLength(5) },
            false
        );
    }

    public void testSeveralIndexGroupsPutRoleRequest() {
        new ReservedRolesStore();

        final ManageRolesPrivilege privilege = new ManageRolesPrivilege(Sets.newHashSet("a*", "b*"));
        final ClusterPermission permission = privilege.buildPermission(new ClusterPermission.Builder()).build();

        assertAllowedIndexPatterns(permission, new String[] { "/[ab].*/" }, true);
        assertAllowedIndexPatterns(permission, new String[] { "/[abc].*/" }, false);
    }

    public void testRestrictedIndexPutRoleRequest() {
        new ReservedRolesStore();

        final ManageRolesPrivilege privilege = new ManageRolesPrivilege(Sets.newHashSet("*"));
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

    private static boolean permissionCheck(ClusterPermission permission, String action, ActionRequest request) {
        final Authentication authentication = AuthenticationTestHelper.builder().build();
        assertThat(request.validate(), nullValue());
        return permission.check(action, request, authentication);
    }

    private static void assertAllowedIndexPatterns(ClusterPermission permission, String[] indexPatterns, boolean expected) {
        {
            final PutRoleRequest putRoleRequest = new PutRoleRequest();
            putRoleRequest.name(randomAlphaOfLength(3));
            putRoleRequest.addIndex(indexPatterns, new String[] { "index", "write", "indices:data/read" }, null, null, null, false);
            assertThat(permissionCheck(permission, "cluster:admin/xpack/security/role/put", putRoleRequest), is(expected));
        }
        {
            final BulkPutRolesRequest bulkPutRolesRequest = new BulkPutRolesRequest(
                List.of(
                    new RoleDescriptor(
                        randomAlphaOfLength(3),
                        new String[] {},
                        new RoleDescriptor.IndicesPrivileges[] {
                            RoleDescriptor.IndicesPrivileges.builder()
                                .indices(indexPatterns)
                                .privileges("read", "read_cross_cluster", "view_index_metadata")
                                .build() },
                        new String[] {}
                    )
                )
            );
            assertThat(permissionCheck(permission, "cluster:admin/xpack/security/role/bulk_put", bulkPutRolesRequest), is(expected));
        }
        // Deletes do not contain patterns, but still need to make sure index name is within permissions
        {
            final BulkDeleteRolesRequest bulkDeleteRolesRequest = new BulkDeleteRolesRequest(List.of(indexPatterns));
            assertThat(permissionCheck(permission, "cluster:admin/xpack/security/role/bulk_delete", bulkDeleteRolesRequest), is(expected));
        }
        {
            assertThat(Arrays.stream(indexPatterns).allMatch(pattern -> {
                final DeleteRoleRequest deleteRolesRequest = new DeleteRoleRequest();
                deleteRolesRequest.name(pattern);
                return permissionCheck(permission, "cluster:admin/xpack/security/role/delete", deleteRolesRequest);
            }), is(expected));
        }
    }

    private static ManageRolesPrivilege buildPrivileges() {
        return buildPrivileges(randomIntBetween(4, 7));
    }

    private static ManageRolesPrivilege buildPrivileges(int indexNameLength) {
        Set<String> indexNames = Sets.newHashSet(
            Arrays.asList(Objects.requireNonNull(generateRandomStringArray(5, indexNameLength, false, false)))
        );
        return new ManageRolesPrivilege(indexNames);
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
        if (instance instanceof ManageRolesPrivilege manageRolesPrivilege) {
            return buildPrivileges(manageRolesPrivilege.getIndices().stream().findFirst().orElse("").length() + randomIntBetween(1, 3));
        }
        fail();
        return null;
    }
}

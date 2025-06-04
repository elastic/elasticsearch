/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.apikey;

import org.elasticsearch.core.Strings;
import org.elasticsearch.exception.ElasticsearchParseException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.security.authc.AuthenticationTestHelper;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.permission.ClusterPermission;
import org.elasticsearch.xpack.core.security.authz.permission.RemoteClusterPermissions;
import org.elasticsearch.xpack.core.security.authz.privilege.ClusterPrivilege;
import org.elasticsearch.xpack.core.security.authz.privilege.ClusterPrivilegeResolver;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.xpack.core.security.action.apikey.CrossClusterApiKeyRoleDescriptorBuilder.CCS_CLUSTER_PRIVILEGE_NAMES;
import static org.elasticsearch.xpack.core.security.action.apikey.CrossClusterApiKeyRoleDescriptorBuilder.ROLE_DESCRIPTOR_NAME;
import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class CrossClusterApiKeyRoleDescriptorBuilderTests extends ESTestCase {

    public void testBuildForSearchOnly() throws IOException {
        final CrossClusterApiKeyRoleDescriptorBuilder access = parseForAccess("""
            {
              "search": [
                {
                  "names": ["metrics"]
                }
              ]
            }""");

        final RoleDescriptor roleDescriptor = access.build();

        assertRoleDescriptor(
            roleDescriptor,
            new String[] { "cross_cluster_search", "monitor_enrich" },
            new RoleDescriptor.IndicesPrivileges[] {
                RoleDescriptor.IndicesPrivileges.builder()
                    .indices("metrics")
                    .privileges("read", "read_cross_cluster", "view_index_metadata")
                    .build() }
        );
    }

    public void testBuildForSearchWithDls() throws IOException {
        final CrossClusterApiKeyRoleDescriptorBuilder access = parseForAccess("""
            {
              "search": [
                {
                  "names": ["metrics"],
                  "query": {"term":{"tag":42}}
                }
              ]
            }""");

        final RoleDescriptor roleDescriptor = access.build();

        assertRoleDescriptor(
            roleDescriptor,
            new String[] { "cross_cluster_search", "monitor_enrich" },
            new RoleDescriptor.IndicesPrivileges[] {
                RoleDescriptor.IndicesPrivileges.builder()
                    .indices("metrics")
                    .privileges("read", "read_cross_cluster", "view_index_metadata")
                    .query("{\"term\":{\"tag\":42}}")
                    .build() }
        );
    }

    public void testBuildForSearchWithFls() throws IOException {
        final CrossClusterApiKeyRoleDescriptorBuilder access = parseForAccess("""
            {
              "search": [
                {
                  "names": ["metrics"],
                  "field_security": {
                    "grant": ["*"],
                    "except": ["private"]
                  }
                }
              ]
            }""");

        final RoleDescriptor roleDescriptor = access.build();

        assertRoleDescriptor(
            roleDescriptor,
            new String[] { "cross_cluster_search", "monitor_enrich" },
            new RoleDescriptor.IndicesPrivileges[] {
                RoleDescriptor.IndicesPrivileges.builder()
                    .indices("metrics")
                    .privileges("read", "read_cross_cluster", "view_index_metadata")
                    .grantedFields("*")
                    .deniedFields("private")
                    .build() }
        );
    }

    public void testBuildForReplicationOnly() throws IOException {
        final CrossClusterApiKeyRoleDescriptorBuilder access = parseForAccess("""
            {
              "replication": [
                {
                  "names": ["archive"]
                }
              ]
            }""");

        final RoleDescriptor roleDescriptor = access.build();

        assertRoleDescriptor(
            roleDescriptor,
            new String[] { "cross_cluster_replication" },
            new RoleDescriptor.IndicesPrivileges[] {
                RoleDescriptor.IndicesPrivileges.builder()
                    .indices("archive")
                    .privileges("cross_cluster_replication", "cross_cluster_replication_internal")
                    .build() }
        );
    }

    public void testBuildForSearchAndReplication() throws IOException {
        final CrossClusterApiKeyRoleDescriptorBuilder access = parseForAccess("""
            {
              "search": [
                {
                  "names": ["metrics"]
                },
                {
                  "names": ["logs"]
                }
              ],
              "replication": [
                {
                  "names": [ "archive" ],
                  "allow_restricted_indices": true
                }
              ]
            }""");

        final RoleDescriptor roleDescriptor = access.build();

        assertRoleDescriptor(
            roleDescriptor,
            new String[] { "cross_cluster_search", "cross_cluster_replication", "monitor_enrich" },
            new RoleDescriptor.IndicesPrivileges[] {
                RoleDescriptor.IndicesPrivileges.builder()
                    .indices("metrics")
                    .privileges("read", "read_cross_cluster", "view_index_metadata")
                    .build(),
                RoleDescriptor.IndicesPrivileges.builder()
                    .indices("logs")
                    .privileges("read", "read_cross_cluster", "view_index_metadata")
                    .build(),
                RoleDescriptor.IndicesPrivileges.builder()
                    .indices("archive")
                    .privileges("cross_cluster_replication", "cross_cluster_replication_internal")
                    .allowRestrictedIndices(true)
                    .build() }
        );
    }

    public void testBuildForSearchAndReplicationWithDLSandFLS() throws IOException {
        // DLS
        CrossClusterApiKeyRoleDescriptorBuilder access = parseForAccess("""
            {
              "search": [
                {
                  "names": ["metrics"],
                  "query": {"term":{"tag":42}}
                }
              ],
              "replication": [
                {
                  "names": [ "archive" ]
                }
              ]
            }""");

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, access::build);
        assertThat(
            exception.getMessage(),
            containsString("search does not support document or field level security if " + "replication is assigned")
        );

        // FLS
        access = parseForAccess("""
            {
              "search": [
                {
                  "names": ["metrics"],
                   "field_security": {
                      "grant": ["*"],
                      "except": ["private"]
                  }
                }
              ],
              "replication": [
                {
                  "names": [ "archive" ]
                }
              ]
            }""");
        exception = expectThrows(IllegalArgumentException.class, access::build);
        assertThat(
            exception.getMessage(),
            containsString("search does not support document or field level security if " + "replication is assigned")
        );

        // DLS and FLS
        access = parseForAccess("""
            {
              "search": [
                {
                  "names": ["metrics"],
                   "query": {"term":{"tag":42}},
                   "field_security": {
                      "grant": ["*"],
                      "except": ["private"]
                  }
                }
              ],
              "replication": [
                {
                  "names": [ "archive" ]
                }
              ]
            }""");

        exception = expectThrows(IllegalArgumentException.class, access::build);
        assertThat(
            exception.getMessage(),
            containsString("search does not support document or field level security if " + "replication is assigned")
        );
    }

    public void testCheckForInvalidLegacyRoleDescriptors() {
        // legacy here is in reference to RCS API privileges pre GA, we know which privileges are used in those versions and is used for
        // minor optimizations. the "legacy" privileges might also be the same as in newer versions, and that is OK too.
        final String[] legacyClusterPrivileges_searchAndReplication = { "cross_cluster_search", "cross_cluster_replication" };
        final String[] legacyClusterPrivileges_searchOnly = { "cross_cluster_search" };
        final String[] legacyIndexPrivileges = { "read", "read_cross_cluster", "view_index_metadata" };
        final String[] otherPrivileges = randomArray(1, 5, String[]::new, () -> randomAlphaOfLength(5));
        String apiKeyId = randomAlphaOfLength(5);
        RoleDescriptor.IndicesPrivileges legacySearchIndexPrivileges_noDLS = RoleDescriptor.IndicesPrivileges.builder()
            .indices(randomAlphaOfLength(5))
            .privileges(legacyIndexPrivileges)
            .build();
        RoleDescriptor.IndicesPrivileges legacySearchIndexPrivileges_withDLS = RoleDescriptor.IndicesPrivileges.builder()
            .indices(randomAlphaOfLength(5))
            .privileges(legacyIndexPrivileges)
            .query("{\"term\":{\"tag\":42}}")
            .build();
        RoleDescriptor.IndicesPrivileges otherIndexPrivilege = RoleDescriptor.IndicesPrivileges.builder()
            .indices(randomAlphaOfLength(5))
            .privileges(otherPrivileges) // replication has fixed index privileges, but for this test we don't care about the actual values
            .build();

        // role descriptor emulates pre GA with search and replication with DLS: this is the primary case we are trying to catch
        RoleDescriptor legacyApiKeyRoleDescriptor_withSearchAndReplication_withDLS = new RoleDescriptor(
            ROLE_DESCRIPTOR_NAME,
            legacyClusterPrivileges_searchAndReplication,
            new RoleDescriptor.IndicesPrivileges[] { legacySearchIndexPrivileges_withDLS, otherIndexPrivilege },
            null
        );
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> CrossClusterApiKeyRoleDescriptorBuilder.checkForInvalidLegacyRoleDescriptors(
                apiKeyId,
                List.of(legacyApiKeyRoleDescriptor_withSearchAndReplication_withDLS)
            )
        );
        assertThat(
            exception.getMessage(),
            equalTo(
                "Cross cluster API key ["
                    + apiKeyId
                    + "] is invalid: search does not support document or field level security if replication is assigned"
            )
        );
        // role descriptor emulates search only with DLS, this could be a valid role descriptor for pre/post GA
        RoleDescriptor apiKeyRoleDescriptor_withSearch_withDLS = new RoleDescriptor(
            ROLE_DESCRIPTOR_NAME,
            legacyClusterPrivileges_searchOnly,
            new RoleDescriptor.IndicesPrivileges[] { legacySearchIndexPrivileges_withDLS },
            null
        );
        noErrorCheckRoleDescriptor(apiKeyRoleDescriptor_withSearch_withDLS);

        // role descriptor emulates search and replication without DLS, this could be a valid role descriptor for pre/post GA
        RoleDescriptor apiKeyRoleDescriptor_withSearchAndReplication_noDLS = new RoleDescriptor(
            ROLE_DESCRIPTOR_NAME,
            legacyClusterPrivileges_searchAndReplication,
            new RoleDescriptor.IndicesPrivileges[] { legacySearchIndexPrivileges_noDLS, otherIndexPrivilege },
            null
        );
        noErrorCheckRoleDescriptor(apiKeyRoleDescriptor_withSearchAndReplication_noDLS);

        // role descriptor that will never have search and replication with DLS but may have other privileges
        RoleDescriptor notLegacyApiKeyRoleDescriptor_withSearchAndReplication_DLS = new RoleDescriptor(
            ROLE_DESCRIPTOR_NAME,
            otherPrivileges,
            new RoleDescriptor.IndicesPrivileges[] { otherIndexPrivilege, otherIndexPrivilege },
            null
        );
        noErrorCheckRoleDescriptor(notLegacyApiKeyRoleDescriptor_withSearchAndReplication_DLS);
    }

    private void noErrorCheckRoleDescriptor(RoleDescriptor roleDescriptor) {
        // should not raise an exception
        CrossClusterApiKeyRoleDescriptorBuilder.checkForInvalidLegacyRoleDescriptors(randomAlphaOfLength(5), List.of(roleDescriptor));
    }

    public void testExplicitlySpecifyingPrivilegesIsNotAllowed() {
        final XContentParseException e = expectThrows(XContentParseException.class, () -> parseForAccess(Strings.format("""
            {
              "%s": [
                {
                  "names": ["metrics"],
                  "privileges": ["read"]
                }
              ]
            }""", randomFrom("search", "replication"))));

        final Throwable cause = e.getCause();
        assertThat(cause, instanceOf(ElasticsearchParseException.class));
        assertThat(
            cause.getMessage(),
            containsString("failed to parse indices privileges for role [cross_cluster]. field [privileges] must not present")
        );
    }

    public void testEmptyAccessIsNotAllowed() throws IOException {
        final CrossClusterApiKeyRoleDescriptorBuilder access1 = parseForAccess(
            randomFrom("{}", "{\"search\":[]}", "{\"replication\":[]}", "{\"search\":[],\"replication\":[]}")
        );
        final IllegalArgumentException e1 = expectThrows(IllegalArgumentException.class, access1::build);
        assertThat(e1.getMessage(), containsString("must specify non-empty access for either [search] or [replication]"));

        final XContentParseException e2 = expectThrows(
            XContentParseException.class,
            () -> parseForAccess(randomFrom("{\"search\":null}", "{\"replication\":null}", "{\"search\":null,\"replication\":null}"))
        );
        assertThat(e2.getMessage(), containsString("doesn't support values of type: VALUE_NULL"));
    }

    public void testAPIKeyAllowsAllRemoteClusterPrivilegesForCCS() {
        // test to help ensure that at least 1 action that is allowed by the remote cluster permissions are supported by CCS
        List<String> actionsToTest = List.of("cluster:monitor/xpack/enrich/esql/resolve_policy", "cluster:monitor/stats/remote");
        // if you add new remote cluster permissions, please define an action we can test to help ensure it is supported by RCS 2.0
        assertThat(actionsToTest.size(), equalTo(RemoteClusterPermissions.getSupportedRemoteClusterPermissions().size()));

        for (String privilege : RemoteClusterPermissions.getSupportedRemoteClusterPermissions()) {
            boolean actionPassesRemoteClusterPermissionCheck = false;
            ClusterPrivilege clusterPrivilege = ClusterPrivilegeResolver.resolve(privilege);
            // each remote cluster privilege has an action to test
            for (String action : actionsToTest) {
                if (clusterPrivilege.buildPermission(ClusterPermission.builder())
                    .build()
                    .check(action, mock(TransportRequest.class), AuthenticationTestHelper.builder().build())) {
                    actionPassesRemoteClusterPermissionCheck = true;
                    break;
                }
            }
            assertTrue(
                "privilege [" + privilege + "] does not cover any actions among [" + actionsToTest + "]",
                actionPassesRemoteClusterPermissionCheck
            );
        }
        // test that the actions pass the privilege check for CCS
        for (String privilege : Set.of(CCS_CLUSTER_PRIVILEGE_NAMES)) {
            boolean actionPassesRemoteCCSCheck = false;
            ClusterPrivilege clusterPrivilege = ClusterPrivilegeResolver.resolve(privilege);
            for (String action : actionsToTest) {
                if (clusterPrivilege.buildPermission(ClusterPermission.builder())
                    .build()
                    .check(action, mock(TransportRequest.class), AuthenticationTestHelper.builder().build())) {
                    actionPassesRemoteCCSCheck = true;
                    break;
                }
            }
            assertTrue(actionPassesRemoteCCSCheck);
        }
    }

    private static void assertRoleDescriptor(
        RoleDescriptor roleDescriptor,
        String[] clusterPrivileges,
        RoleDescriptor.IndicesPrivileges[] indicesPrivileges
    ) {
        assertThat(roleDescriptor.getName().equals("cross_cluster"), is(true));
        assertThat(roleDescriptor.getClusterPrivileges(), arrayContainingInAnyOrder(clusterPrivileges));
        assertThat(roleDescriptor.getIndicesPrivileges(), equalTo(indicesPrivileges));
        CrossClusterApiKeyRoleDescriptorBuilder.validate(roleDescriptor);
    }

    private static CrossClusterApiKeyRoleDescriptorBuilder parseForAccess(String content) throws IOException {
        return CrossClusterApiKeyRoleDescriptorBuilder.PARSER.parse(
            JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, content),
            null
        );
    }
}

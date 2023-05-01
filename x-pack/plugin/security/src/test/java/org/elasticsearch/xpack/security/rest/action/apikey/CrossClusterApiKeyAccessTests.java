/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.rest.action.apikey;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.core.Strings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;

import java.io.IOException;

import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class CrossClusterApiKeyAccessTests extends ESTestCase {

    public void testToRoleDescriptorSearchOnly() throws IOException {
        final CrossClusterApiKeyAccess access = parseForAccess("""
            {
              "search": [
                {
                  "names": ["metrics"]
                }
              ]
            }""");

        final String name = randomAlphaOfLengthBetween(3, 8);
        final RoleDescriptor roleDescriptor = access.toRoleDescriptor(name);

        assertRoleDescriptor(
            roleDescriptor,
            name,
            new String[] { "cross_cluster_access" },
            new RoleDescriptor.IndicesPrivileges[] {
                RoleDescriptor.IndicesPrivileges.builder()
                    .indices("metrics")
                    .privileges("read", "read_cross_cluster", "view_index_metadata")
                    .build() }
        );
    }

    public void testToRoleDescriptorReplicationOnly() throws IOException {
        final CrossClusterApiKeyAccess access = parseForAccess("""
            {
              "replication": [
                {
                  "names": ["archive"]
                }
              ]
            }""");

        final String name = randomAlphaOfLengthBetween(3, 8);
        final RoleDescriptor roleDescriptor = access.toRoleDescriptor(name);

        assertRoleDescriptor(
            roleDescriptor,
            name,
            new String[] { "cross_cluster_access", "cluster:monitor/state" },
            new RoleDescriptor.IndicesPrivileges[] {
                RoleDescriptor.IndicesPrivileges.builder()
                    .indices("archive")
                    .privileges(
                        "manage",
                        "read",
                        "indices:internal/admin/ccr/restore/*",
                        "internal:transport/proxy/indices:internal/admin/ccr/restore/*"
                    )
                    .build() }
        );
    }

    public void testToRolDescriptorSearchAndReplication() throws IOException {
        final CrossClusterApiKeyAccess access = parseForAccess("""
            {
              "search": [
                {
                  "names": ["metrics"],
                  "query": {"term":{"tag":42}}
                },
                {
                  "names": ["logs"],
                  "field_security": {
                    "grant": ["*"],
                    "except": ["private"]
                  }
                }
              ],
              "replication": [
                {
                  "names": [ "archive" ],
                  "allow_restricted_indices": true
                }
              ]
            }""");

        final String name = randomAlphaOfLengthBetween(3, 8);
        final RoleDescriptor roleDescriptor = access.toRoleDescriptor(name);

        assertRoleDescriptor(
            roleDescriptor,
            name,
            new String[] { "cross_cluster_access", "cluster:monitor/state" },
            new RoleDescriptor.IndicesPrivileges[] {
                RoleDescriptor.IndicesPrivileges.builder()
                    .indices("metrics")
                    .privileges("read", "read_cross_cluster", "view_index_metadata")
                    .query("{\"term\":{\"tag\":42}}")
                    .build(),
                RoleDescriptor.IndicesPrivileges.builder()
                    .indices("logs")
                    .privileges("read", "read_cross_cluster", "view_index_metadata")
                    .grantedFields("*")
                    .deniedFields("private")
                    .build(),
                RoleDescriptor.IndicesPrivileges.builder()
                    .indices("archive")
                    .privileges(
                        "manage",
                        "read",
                        "indices:internal/admin/ccr/restore/*",
                        "internal:transport/proxy/indices:internal/admin/ccr/restore/*"
                    )
                    .allowRestrictedIndices(true)
                    .build() }
        );
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
        final CrossClusterApiKeyAccess access1 = parseForAccess(
            randomFrom("{}", "{\"search\":[]}", "{\"replication\":[]}", "{\"search\":[],\"replication\":[]}")
        );
        final IllegalArgumentException e1 = expectThrows(
            IllegalArgumentException.class,
            () -> access1.toRoleDescriptor(randomAlphaOfLengthBetween(3, 8))
        );
        assertThat(e1.getMessage(), containsString("must specify non-empty access for either [search] or [replication]"));

        final XContentParseException e2 = expectThrows(
            XContentParseException.class,
            () -> parseForAccess(randomFrom("{\"search\":null}", "{\"replication\":null}", "{\"search\":null,\"replication\":null}"))
        );
        assertThat(e2.getMessage(), containsString("doesn't support values of type: VALUE_NULL"));
    }

    private static void assertRoleDescriptor(
        RoleDescriptor roleDescriptor,
        String name,
        String[] clusterPrivileges,
        RoleDescriptor.IndicesPrivileges[] indicesPrivileges
    ) {
        assertThat(roleDescriptor.getName().equals(name), is(true));
        assertThat(roleDescriptor.hasApplicationPrivileges(), is(false));
        assertThat(roleDescriptor.hasRunAs(), is(false));
        assertThat(roleDescriptor.hasConfigurableClusterPrivileges(), is(false));
        assertThat(roleDescriptor.hasRemoteIndicesPrivileges(), is(false));

        assertThat(roleDescriptor.getClusterPrivileges(), arrayContainingInAnyOrder(clusterPrivileges));
        assertThat(roleDescriptor.getIndicesPrivileges(), equalTo(indicesPrivileges));
    }

    private static CrossClusterApiKeyAccess parseForAccess(String content) throws IOException {
        return CrossClusterApiKeyAccess.PARSER.parse(
            JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, content),
            null
        );
    }
}

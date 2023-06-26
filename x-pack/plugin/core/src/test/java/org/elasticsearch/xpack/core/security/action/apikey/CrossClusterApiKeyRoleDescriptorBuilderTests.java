/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.apikey;

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
            new String[] { "cross_cluster_search" },
            new RoleDescriptor.IndicesPrivileges[] {
                RoleDescriptor.IndicesPrivileges.builder()
                    .indices("metrics")
                    .privileges("read", "read_cross_cluster", "view_index_metadata")
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

        final RoleDescriptor roleDescriptor = access.build();

        assertRoleDescriptor(
            roleDescriptor,
            new String[] { "cross_cluster_search", "cross_cluster_replication" },
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
                    .privileges("cross_cluster_replication", "cross_cluster_replication_internal")
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

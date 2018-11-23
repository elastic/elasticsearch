/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.client.security;

import org.elasticsearch.client.security.user.privileges.IndicesPrivileges;
import org.elasticsearch.client.security.user.privileges.Role;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.equalTo;

public class GetRolesResponseTests extends ESTestCase {

    public void testFromXContent() throws IOException {
        String json =
            "{\n" +
                "  \"my_admin_role\": {\n" +
                "    \"cluster\" : [ \"all\" ],\n" +
                "    \"indices\" : [\n" +
                "      {\n" +
                "        \"names\" : [ \"index1\", \"index2\" ],\n" +
                "        \"privileges\" : [ \"all\" ],\n" +
                "        \"field_security\" : {\n" +
                "          \"grant\" : [ \"title\", \"body\" ]}\n" +
                "      }\n" +
                "    ],\n" +
                "    \"applications\" : [ ],\n" +
                "    \"run_as\" : [ \"other_user\" ],\n" +
                "    \"metadata\" : {\n" +
                "      \"version\" : 1\n" +
                "    },\n" +
                "    \"transient_metadata\" : {\n" +
                "      \"enabled\" : true\n" +
                "    }\n" +
                "  }\n" +
                "}";
        final GetRolesResponse response = GetRolesResponse.fromXContent((XContentType.JSON.xContent().createParser(
            new NamedXContentRegistry(Collections.emptyList()), new DeprecationHandler() {
                @Override
                public void usedDeprecatedName(String usedName, String modernName) {
                }

                @Override
                public void usedDeprecatedField(String usedName, String replacedWith) {
                }
            }, json)));
        assertThat(response.getRoles().size(), equalTo(1));
        final Role role = response.getRoles().get(0);
        assertThat(role.getName(), equalTo("my_admin_role"));
        assertThat(role.getClusterPrivileges().size(), equalTo(1));
        IndicesPrivileges expectedIndicesPrivileges = new IndicesPrivileges.Builder()
            .indices("index1", "index2")
            .privileges("all")
            .grantedFields("title", "body")
            .build();
        assertThat(role.getIndicesPrivileges().contains(expectedIndicesPrivileges), equalTo(true));
        final Map<String, Object> expectedMetadata = new HashMap<>();
        expectedMetadata.put("version", 1);
        final Map<String, Object> expectedTransientMetadata = new HashMap<>();
        expectedTransientMetadata.put("enabled", true);
        final Role expectedRole = Role.builder()
            .name("my_admin_role")
            .clusterPrivileges("all")
            .indicesPrivileges(expectedIndicesPrivileges)
            .runAsPrivilege("other_user")
            .metadata(expectedMetadata)
            .transientMetadata(expectedTransientMetadata)
            .build();
        assertThat(role, equalTo(expectedRole));
    }

    public void testEqualsHashCode() {
        final List<Role> roles = new ArrayList<>();
        IndicesPrivileges indicesPrivileges = new IndicesPrivileges.Builder()
            .indices("index1", "index2")
            .privileges("write", "monitor", "delete")
            .grantedFields("field1", "field2")
            .deniedFields("field3", "field4")
            .build();
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("key", "value");
        Map<String, Object> transientMetadata = new HashMap<>();
        transientMetadata.put("transient_key", "transient_value");
        final Role role = Role.builder()
            .name("role_name")
            .clusterPrivileges("monitor", "manage", "manage_saml")
            .indicesPrivileges(indicesPrivileges)
            .runAsPrivilege("run_as_user")
            .metadata(metadata)
            .transientMetadata(transientMetadata)
            .build();
        roles.add(role);
        IndicesPrivileges indicesPrivileges2 = new IndicesPrivileges.Builder()
            .indices("other_index1", "other_index2")
            .privileges("write", "monitor", "delete")
            .grantedFields("other_field1", "other_field2")
            .deniedFields("other_field3", "other_field4")
            .build();
        Map<String, Object> metadata2 = new HashMap<>();
        metadata.put("other_key", "other_value");
        Map<String, Object> transientMetadata2 = new HashMap<>();
        transientMetadata2.put("other_transient_key", "other_transient_value");
        final Role role2 = Role.builder()
            .name("role2_name")
            .clusterPrivileges("monitor", "manage", "manage_saml")
            .indicesPrivileges(indicesPrivileges2)
            .runAsPrivilege("other_run_as_user")
            .metadata(metadata2)
            .transientMetadata(transientMetadata2)
            .build();
        roles.add(role2);
        final GetRolesResponse getRolesResponse = new GetRolesResponse(roles);
        assertNotNull(getRolesResponse);
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(getRolesResponse, (original) -> {
            return new GetRolesResponse(original.getRoles());
        });
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(getRolesResponse, (original) -> {
            return new GetRolesResponse(original.getRoles());
        }, GetRolesResponseTests::mutateTestItem);

    }

    private static GetRolesResponse mutateTestItem(GetRolesResponse original) {
        if (randomBoolean()) {
            final List<Role> roles = new ArrayList<>();
            IndicesPrivileges indicesPrivileges = new IndicesPrivileges.Builder()
                .indices("index1", "index2")
                .privileges("write", "monitor", "delete")
                .grantedFields("field1", "field2")
                .deniedFields("field3", "field4")
                .build();
            Map<String, Object> metadata = new HashMap<String, Object>();
            metadata.put("key", "value");
            Map<String, Object> transientMetadata = new HashMap<>();
            transientMetadata.put("transient_key", "transient_value");
            final Role role = Role.builder()
                .name("role_name")
                .clusterPrivileges("monitor", "manage", "manage_saml")
                .indicesPrivileges(indicesPrivileges)
                .runAsPrivilege("run_as_user")
                .metadata(metadata)
                .transientMetadata(transientMetadata)
                .build();
            roles.add(role);
            return new GetRolesResponse(roles);
        } else {
            IndicesPrivileges indicesPrivileges = new IndicesPrivileges.Builder()
                .indices("index1_changed", "index2")
                .privileges("write", "monitor", "delete")
                .grantedFields("field1", "field2")
                .deniedFields("field3", "field4")
                .build();
            Map<String, Object> metadata = new HashMap<String, Object>();
            metadata.put("key", "value");
            Map<String, Object> transientMetadata = new HashMap<>();
            transientMetadata.put("transient_key", "transient_value");
            final Role role = Role.builder()
                .name("role_name")
                .clusterPrivileges("monitor", "manage", "manage_saml")
                .indicesPrivileges(indicesPrivileges)
                .runAsPrivilege("run_as_user")
                .metadata(metadata)
                .transientMetadata(transientMetadata)
                .build();
            List<Role> newRoles = original.getRoles().stream().collect(Collectors.toList());
            newRoles.remove(0);
            newRoles.add(role);
            return new GetRolesResponse(newRoles);
        }
    }
}

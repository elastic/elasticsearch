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

import org.elasticsearch.client.security.support.expressiondsl.RoleMapperExpression;
import org.elasticsearch.client.security.support.expressiondsl.fields.FieldRoleMapperExpression;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.equalTo;

public class PutRoleMappingRequestTests extends ESTestCase {

    public void testPutRoleMappingRequest() {
        final String name = randomAlphaOfLength(5);
        final boolean enabled = randomBoolean();
        final List<String> roles = Collections.singletonList("superuser");
        final RoleMapperExpression rules = FieldRoleMapperExpression.ofUsername("user");
        final Map<String, Object> metadata = new HashMap<>();
        metadata.put("k1", "v1");
        final RefreshPolicy refreshPolicy = randomFrom(RefreshPolicy.values());

        PutRoleMappingRequest putRoleMappingRequest = new PutRoleMappingRequest(name, enabled, roles, rules, metadata, refreshPolicy);
        assertNotNull(putRoleMappingRequest);
        assertThat(putRoleMappingRequest.getName(), equalTo(name));
        assertThat(putRoleMappingRequest.isEnabled(), equalTo(enabled));
        assertThat(putRoleMappingRequest.getRefreshPolicy(), equalTo((refreshPolicy == null) ? RefreshPolicy.getDefault() : refreshPolicy));
        assertThat(putRoleMappingRequest.getRules(), equalTo(rules));
        assertThat(putRoleMappingRequest.getRoles(), equalTo(roles));
        assertThat(putRoleMappingRequest.getMetadata(), equalTo((metadata == null) ? Collections.emptyMap() : metadata));
    }

    public void testPutRoleMappingRequestThrowsExceptionForNullOrEmptyName() {
        final String name = randomBoolean() ? null : "";
        final boolean enabled = randomBoolean();
        final List<String> roles = Collections.singletonList("superuser");
        final RoleMapperExpression rules = FieldRoleMapperExpression.ofUsername("user");
        final Map<String, Object> metadata = new HashMap<>();
        metadata.put("k1", "v1");
        final RefreshPolicy refreshPolicy = randomFrom(RefreshPolicy.values());

        final IllegalArgumentException ile = expectThrows(IllegalArgumentException.class, () -> new PutRoleMappingRequest(name, enabled,
                roles, rules, metadata, refreshPolicy));
        assertThat(ile.getMessage(), equalTo("role-mapping name is missing"));
    }

    public void testPutRoleMappingRequestThrowsExceptionForNullOrEmptyRoles() {
        final String name = randomAlphaOfLength(5);
        final boolean enabled = randomBoolean();
        final List<String> roles = randomBoolean() ? null : Collections.emptyList();
        final RoleMapperExpression rules = FieldRoleMapperExpression.ofUsername("user");
        final Map<String, Object> metadata = new HashMap<>();
        metadata.put("k1", "v1");
        final RefreshPolicy refreshPolicy = randomFrom(RefreshPolicy.values());

        final IllegalArgumentException ile = expectThrows(IllegalArgumentException.class, () -> new PutRoleMappingRequest(name, enabled,
                roles, rules, metadata, refreshPolicy));
        assertThat(ile.getMessage(), equalTo("role-mapping roles are missing"));
    }

    public void testPutRoleMappingRequestThrowsExceptionForNullRules() {
        final String name = randomAlphaOfLength(5);
        final boolean enabled = randomBoolean();
        final List<String> roles = Collections.singletonList("superuser");
        final RoleMapperExpression rules = null;
        final Map<String, Object> metadata = new HashMap<>();
        metadata.put("k1", "v1");
        final RefreshPolicy refreshPolicy = randomFrom(RefreshPolicy.values());

        expectThrows(NullPointerException.class, () -> new PutRoleMappingRequest(name, enabled, roles, rules, metadata, refreshPolicy));
    }

    public void testPutRoleMappingRequestToXContent() throws IOException {
        final String name = randomAlphaOfLength(5);
        final boolean enabled = randomBoolean();
        final List<String> roles = Collections.singletonList("superuser");
        final RoleMapperExpression rules = FieldRoleMapperExpression.ofUsername("user");
        final Map<String, Object> metadata = new HashMap<>();
        metadata.put("k1", "v1");
        final RefreshPolicy refreshPolicy = randomFrom(RefreshPolicy.values());

        final PutRoleMappingRequest putRoleMappingRequest = new PutRoleMappingRequest(name, enabled, roles, rules, metadata, refreshPolicy);

        final XContentBuilder builder = XContentFactory.jsonBuilder();
        putRoleMappingRequest.toXContent(builder, ToXContent.EMPTY_PARAMS);
        final String output = Strings.toString(builder);
        final String expected =
             "{"+
               "\"enabled\":" + enabled + "," +
               "\"roles\":[\"superuser\"]," +
               "\"rules\":{" +
                   "\"field\":{\"username\":[\"user\"]}" +
               "}," +
               "\"metadata\":{\"k1\":\"v1\"}" +
             "}";

        assertThat(output, equalTo(expected));
    }

    public void testEqualsHashCode() {
        final String name = randomAlphaOfLength(5);
        final boolean enabled = randomBoolean();
        final List<String> roles = Collections.singletonList("superuser");
        final RoleMapperExpression rules = FieldRoleMapperExpression.ofUsername("user");
        final Map<String, Object> metadata = new HashMap<>();
        metadata.put("k1", "v1");
        final RefreshPolicy refreshPolicy = randomFrom(RefreshPolicy.values());

        PutRoleMappingRequest putRoleMappingRequest = new PutRoleMappingRequest(name, enabled, roles, rules, metadata, refreshPolicy);
        assertNotNull(putRoleMappingRequest);

        EqualsHashCodeTestUtils.checkEqualsAndHashCode(putRoleMappingRequest, (original) -> {
            return new PutRoleMappingRequest(original.getName(), original.isEnabled(), original.getRoles(), original.getRules(), original
                    .getMetadata(), original.getRefreshPolicy());
        });
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(putRoleMappingRequest, (original) -> {
            return new PutRoleMappingRequest(original.getName(), original.isEnabled(), original.getRoles(), original.getRules(), original
                    .getMetadata(), original.getRefreshPolicy());
        }, PutRoleMappingRequestTests::mutateTestItem);
    }

    private static PutRoleMappingRequest mutateTestItem(PutRoleMappingRequest original) {
        switch (randomIntBetween(0, 4)) {
        case 0:
            return new PutRoleMappingRequest(randomAlphaOfLength(5), original.isEnabled(), original.getRoles(), original.getRules(),
                    original.getMetadata(), original.getRefreshPolicy());
        case 1:
            return new PutRoleMappingRequest(original.getName(), !original.isEnabled(), original.getRoles(), original.getRules(),
                    original.getMetadata(), original.getRefreshPolicy());
        case 2:
            return new PutRoleMappingRequest(original.getName(), original.isEnabled(), original.getRoles(),
                    FieldRoleMapperExpression.ofGroups("group"), original.getMetadata(), original.getRefreshPolicy());
        case 3:
            return new PutRoleMappingRequest(original.getName(), original.isEnabled(), original.getRoles(), original.getRules(),
                    Collections.emptyMap(), original.getRefreshPolicy());
        case 4:
            List<RefreshPolicy> values = Arrays.stream(RefreshPolicy.values())
                                                .filter(rp -> rp != original.getRefreshPolicy())
                                                .collect(Collectors.toList());
            return new PutRoleMappingRequest(original.getName(), original.isEnabled(), original.getRoles(), original.getRules(), original
                    .getMetadata(), randomFrom(values));
        default:
            return new PutRoleMappingRequest(randomAlphaOfLength(5), original.isEnabled(), original.getRoles(), original.getRules(),
                    original.getMetadata(), original.getRefreshPolicy());
        }
    }

}

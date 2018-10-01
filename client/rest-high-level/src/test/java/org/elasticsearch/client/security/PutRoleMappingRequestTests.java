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
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class PutRoleMappingRequestTests extends ESTestCase {

    public void testPutRoleMappingRequest() {
        final String name = randomFrom(randomAlphaOfLength(5), null, "");
        final boolean enabled = randomBoolean();
        final boolean nullorEmptyRoles = randomBoolean();
        final List<String> roles = nullorEmptyRoles ? (randomBoolean() ? null : Collections.emptyList())
                : Collections.singletonList("superuser");
        final RoleMapperExpression rules = randomFrom(FieldRoleMapperExpression.ofUsername("user"), null);
        final boolean nullorEmptyMetadata = randomBoolean();
        final Map<String, Object> metadata;
        if (nullorEmptyMetadata) {
            metadata = randomBoolean() ? null : Collections.emptyMap();
        } else {
            metadata = new HashMap<>();
            metadata.put("k1", "v1");
        }
        final RefreshPolicy refreshPolicy = randomFrom(randomFrom(RefreshPolicy.values()), null);

        if (Strings.hasText(name) == false) {
            final IllegalArgumentException ile = expectThrows(IllegalArgumentException.class, () -> new PutRoleMappingRequest(name, enabled,
                    roles, rules, metadata, refreshPolicy));
            assertThat(ile.getMessage(), equalTo("role-mapping name is missing"));
        } else if (roles == null || roles.isEmpty()) {
            final IllegalArgumentException ile = expectThrows(IllegalArgumentException.class, () -> new PutRoleMappingRequest(name, enabled,
                    roles, rules, metadata, refreshPolicy));
            assertThat(ile.getMessage(), equalTo("role-mapping roles are missing"));
        } else if (rules == null) {
            expectThrows(NullPointerException.class, () -> new PutRoleMappingRequest(name, enabled, roles, rules, metadata, refreshPolicy));
        } else {
            PutRoleMappingRequest putRoleMappingRequest = new PutRoleMappingRequest(name, enabled, roles, rules, metadata, refreshPolicy);
            assertNotNull(putRoleMappingRequest);
            assertThat(putRoleMappingRequest.getName(), equalTo(name));
            assertThat(putRoleMappingRequest.isEnabled(), equalTo(enabled));
            assertThat(putRoleMappingRequest.getRefreshPolicy(), equalTo((refreshPolicy == null) ? RefreshPolicy.getDefault()
                    : refreshPolicy));
            assertThat(putRoleMappingRequest.getRules(), equalTo(rules));
            assertThat(putRoleMappingRequest.getRoles(), equalTo(roles));
            assertThat(putRoleMappingRequest.getMetadata(), equalTo((metadata == null) ? Collections.emptyMap() : metadata));
        }
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
        switch (randomIntBetween(0, 2)) {
        case 0:
            return new PutRoleMappingRequest(randomAlphaOfLength(5), original.isEnabled(), original.getRoles(), original.getRules(),
                    original.getMetadata(), original.getRefreshPolicy());
        case 1:
            return new PutRoleMappingRequest(original.getName(), !original.isEnabled(), original.getRoles(), original.getRules(), original
                    .getMetadata(), original.getRefreshPolicy());
        case 2:
            return new PutRoleMappingRequest(original.getName(), original.isEnabled(), original.getRoles(), FieldRoleMapperExpression
                    .ofGroups("group"), original.getMetadata(), original.getRefreshPolicy());
        default:
            throw new IllegalArgumentException("unknown option");
        }
    }

}

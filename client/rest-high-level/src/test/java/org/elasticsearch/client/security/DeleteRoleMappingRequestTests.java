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

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.equalTo;

public class DeleteRoleMappingRequestTests extends ESTestCase {

    public void testDeleteRoleMappingRequest() {
        final String name = randomAlphaOfLength(5);
        final RefreshPolicy refreshPolicy = randomFrom(RefreshPolicy.values());
        final DeleteRoleMappingRequest deleteRoleMappingRequest = new DeleteRoleMappingRequest(name, refreshPolicy);
        assertThat(deleteRoleMappingRequest.getName(), equalTo(name));
        assertThat(deleteRoleMappingRequest.getRefreshPolicy(), equalTo(refreshPolicy));
    }

    public void testDeleteRoleMappingRequestThrowsExceptionForNullOrEmptyName() {
        final String name = randomBoolean() ? null : "";
        final IllegalArgumentException ile = expectThrows(IllegalArgumentException.class, () -> new DeleteRoleMappingRequest(name, null));
        assertThat(ile.getMessage(), equalTo("role-mapping name is required"));
    }

    public void testEqualsHashCode() {
        final String name = randomAlphaOfLength(5);
        final RefreshPolicy refreshPolicy = randomFrom(RefreshPolicy.values());
        final DeleteRoleMappingRequest deleteRoleMappingRequest = new DeleteRoleMappingRequest(name, refreshPolicy);
        assertNotNull(deleteRoleMappingRequest);

        EqualsHashCodeTestUtils.checkEqualsAndHashCode(deleteRoleMappingRequest, (original) -> {
            return new DeleteRoleMappingRequest(original.getName(), original.getRefreshPolicy());
        });

        EqualsHashCodeTestUtils.checkEqualsAndHashCode(deleteRoleMappingRequest, (original) -> {
            return new DeleteRoleMappingRequest(original.getName(), original.getRefreshPolicy());
        }, DeleteRoleMappingRequestTests::mutateTestItem);

    }

    private static DeleteRoleMappingRequest mutateTestItem(DeleteRoleMappingRequest original) {
        if (randomBoolean()) {
            return new DeleteRoleMappingRequest(randomAlphaOfLength(5), original.getRefreshPolicy());
        } else {
            List<RefreshPolicy> values = Arrays.stream(RefreshPolicy.values()).filter(rp -> rp != original.getRefreshPolicy()).collect(
                    Collectors.toList());
            return new DeleteRoleMappingRequest(original.getName(), randomFrom(values));
        }
    }
}

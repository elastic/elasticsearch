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

public class DeleteUserRequestTests extends ESTestCase {

    public void testDeleteUserRequest() {
        final String name = randomAlphaOfLength(10);
        final RefreshPolicy refreshPolicy = randomFrom(RefreshPolicy.values());
        final DeleteUserRequest deleteUserRequest = new DeleteUserRequest(name, refreshPolicy);
        assertThat(deleteUserRequest.getName(), equalTo(name));
        assertThat(deleteUserRequest.getRefreshPolicy(), equalTo(refreshPolicy));
    }

    public void testDeleteUserRequestThrowsExceptionForNullName() {
        final NullPointerException ile =
            expectThrows(NullPointerException.class, () -> new DeleteUserRequest(null, randomFrom(RefreshPolicy.values())));
        assertThat(ile.getMessage(), equalTo("user name is required"));
    }

    public void testDeleteUserRequestThrowsExceptionForNullRefreshPolicy() {
        final NullPointerException ile =
            expectThrows(NullPointerException.class, () -> new DeleteUserRequest(randomAlphaOfLength(10), null));
        assertThat(ile.getMessage(), equalTo("refresh policy is required"));
    }

    public void testEqualsHashCode() {
        final String name = randomAlphaOfLength(10);
        final RefreshPolicy refreshPolicy = randomFrom(RefreshPolicy.values());
        final DeleteUserRequest deleteUserRequest = new DeleteUserRequest(name, refreshPolicy);
        assertNotNull(deleteUserRequest);

        EqualsHashCodeTestUtils.checkEqualsAndHashCode(deleteUserRequest, (original) -> {
            return new DeleteUserRequest(original.getName(), original.getRefreshPolicy());
        });

        EqualsHashCodeTestUtils.checkEqualsAndHashCode(deleteUserRequest, (original) -> {
            return new DeleteUserRequest(original.getName(), original.getRefreshPolicy());
        }, DeleteUserRequestTests::mutateTestItem);

    }

    private static DeleteUserRequest mutateTestItem(DeleteUserRequest original) {
        if (randomBoolean()) {
            return new DeleteUserRequest(randomAlphaOfLength(10), original.getRefreshPolicy());
        } else {
            List<RefreshPolicy> values = Arrays.stream(RefreshPolicy.values()).filter(rp -> rp != original.getRefreshPolicy()).collect(
                    Collectors.toList());
            return new DeleteUserRequest(original.getName(), randomFrom(values));
        }
    }
}

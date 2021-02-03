/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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

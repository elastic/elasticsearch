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

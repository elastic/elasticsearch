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

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;

public class GetUsersRequestTests extends ESTestCase {

    public void testGetUsersRequest() {
        final String[] users = randomArray(0, 5, String[]::new, () -> randomAlphaOfLength(5));
        GetUsersRequest getUsersRequest = new GetUsersRequest(users);
        assertThat(getUsersRequest.getUsernames().size(), equalTo(users.length));
        assertThat(getUsersRequest.getUsernames(), containsInAnyOrder(users));
    }

    public void testEqualsHashCode() {
        final String[] users = randomArray(0, 5, String[]::new, () -> randomAlphaOfLength(5));
        final GetUsersRequest getUsersRequest = new GetUsersRequest(users);
        assertNotNull(getUsersRequest);
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(getUsersRequest, (original) -> {
            return new GetUsersRequest(original.getUsernames().toArray(new String[0]));
        });
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(getUsersRequest, (original) -> {
            return new GetUsersRequest(original.getUsernames().toArray(new String[0]));
        }, GetUsersRequestTests::mutateTestItem);
    }

    private static GetUsersRequest mutateTestItem(GetUsersRequest original) {
        final int minRoles = original.getUsernames().isEmpty() ? 1 : 0;
        return new GetUsersRequest(randomArray(minRoles, 5, String[]::new, () -> randomAlphaOfLength(6)));
    }

}

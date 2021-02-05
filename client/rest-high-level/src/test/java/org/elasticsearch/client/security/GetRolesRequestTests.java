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

public class GetRolesRequestTests extends ESTestCase {

    public void testGetRolesRequest() {
        final String[] roles = randomArray(0, 5, String[]::new, () -> randomAlphaOfLength(5));
        final GetRolesRequest getRolesRequest = new GetRolesRequest(roles);
        assertThat(getRolesRequest.getRoleNames().size(), equalTo(roles.length));
        assertThat(getRolesRequest.getRoleNames(), containsInAnyOrder(roles));
    }

    public void testEqualsHashCode() {
        final String[] roles = randomArray(0, 5, String[]::new, () -> randomAlphaOfLength(5));
        final GetRolesRequest getRolesRequest = new GetRolesRequest(roles);
        assertNotNull(getRolesRequest);
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(getRolesRequest, (original) -> {
            return new GetRolesRequest(original.getRoleNames().toArray(new String[0]));
        });
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(getRolesRequest, (original) -> {
            return new GetRolesRequest(original.getRoleNames().toArray(new String[0]));
        }, GetRolesRequestTests::mutateTestItem);
    }

    private static GetRolesRequest mutateTestItem(GetRolesRequest original) {
        final int minRoles = original.getRoleNames().isEmpty() ? 1 : 0;
        return new GetRolesRequest(randomArray(minRoles, 5, String[]::new, () -> randomAlphaOfLength(6)));
    }
}

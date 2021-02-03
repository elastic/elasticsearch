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
import static org.hamcrest.Matchers.is;

public class GetRoleMappingsRequestTests extends ESTestCase {

    public void testGetRoleMappingsRequest() {
        int noOfRoleMappingNames = randomIntBetween(0, 2);
        final String[] roleMappingNames = randomArray(noOfRoleMappingNames, noOfRoleMappingNames, String[]::new, () -> randomAlphaOfLength(
                5));
        final GetRoleMappingsRequest getRoleMappingsRequest = new GetRoleMappingsRequest(roleMappingNames);
        assertThat(getRoleMappingsRequest.getRoleMappingNames().size(), is(noOfRoleMappingNames));
        assertThat(getRoleMappingsRequest.getRoleMappingNames(), containsInAnyOrder(roleMappingNames));
    }

    public void testEqualsHashCode() {
        int noOfRoleMappingNames = randomIntBetween(0, 2);
        final String[] roleMappingNames = randomArray(noOfRoleMappingNames, String[]::new, () -> randomAlphaOfLength(5));
        final GetRoleMappingsRequest getRoleMappingsRequest = new GetRoleMappingsRequest(roleMappingNames);
        assertNotNull(getRoleMappingsRequest);
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(getRoleMappingsRequest, (original) -> {
            return new GetRoleMappingsRequest(original.getRoleMappingNames().toArray(new String[0]));
        });
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(getRoleMappingsRequest, (original) -> {
            return new GetRoleMappingsRequest(original.getRoleMappingNames().toArray(new String[0]));
        }, GetRoleMappingsRequestTests::mutateTestItem);
    }

    private static GetRoleMappingsRequest mutateTestItem(GetRoleMappingsRequest original) {
        return new GetRoleMappingsRequest(randomAlphaOfLength(8));
    }
}

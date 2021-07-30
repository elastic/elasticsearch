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

import static org.hamcrest.Matchers.equalTo;

public class GetPrivilegesRequestTests extends ESTestCase {

    public void testGetPrivilegesRequest() {
        final String applicationName = randomAlphaOfLength(5);
        final int numberOfPrivileges = randomIntBetween(0, 5);
        final String[] privilegeNames = randomBoolean() ? null : randomArray(numberOfPrivileges, numberOfPrivileges, String[]::new,
            () -> randomAlphaOfLength(5));
        final GetPrivilegesRequest getPrivilegesRequest = new GetPrivilegesRequest(applicationName, privilegeNames);
        assertThat(getPrivilegesRequest.getApplicationName(), equalTo(applicationName));
        assertThat(getPrivilegesRequest.getPrivilegeNames(), equalTo(privilegeNames));
    }

    public void testPrivilegeWithoutApplication() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> {
            new GetPrivilegesRequest(null, randomAlphaOfLength(5));
        });
        assertThat(e.getMessage(), equalTo("privilege cannot be specified when application is missing"));
    }

    public void testEqualsAndHashCode() {
        final String applicationName = randomAlphaOfLength(5);
        final int numberOfPrivileges = randomIntBetween(0, 5);
        final String[] privilegeNames =
            randomArray(numberOfPrivileges, numberOfPrivileges, String[]::new, () -> randomAlphaOfLength(5));
        final GetPrivilegesRequest getPrivilegesRequest = new GetPrivilegesRequest(applicationName, privilegeNames);
        final EqualsHashCodeTestUtils.MutateFunction<GetPrivilegesRequest> mutate = r -> {
            if (randomBoolean()) {
                final int numberOfNewPrivileges = randomIntBetween(1, 5);
                final String[] newPrivilegeNames =
                    randomArray(numberOfNewPrivileges, numberOfNewPrivileges, String[]::new, () -> randomAlphaOfLength(5));
                return new GetPrivilegesRequest(applicationName, newPrivilegeNames);
            } else {
                return GetPrivilegesRequest.getApplicationPrivileges(randomAlphaOfLength(6));
            }
        };
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(getPrivilegesRequest,
            r -> new GetPrivilegesRequest(r.getApplicationName(), r.getPrivilegeNames()), mutate);
    }
}

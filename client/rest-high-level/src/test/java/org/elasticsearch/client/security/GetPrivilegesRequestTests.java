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

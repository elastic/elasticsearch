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

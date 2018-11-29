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

import org.elasticsearch.client.security.user.User;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.equalTo;

/** tests the Response for getting users from the security HLRC */
public class GetUsersResponseTests extends ESTestCase {
    public void testFromXContent() throws IOException {
        String json =
            "{\n" +
                "  \"jacknich\": {\n" +
                "    \"username\": \"jacknich\",\n" +
                "    \"roles\": [\n" +
                "      \"admin\", \"other_role1\"\n" +
                "    ],\n" +
                "    \"full_name\": \"Jack Nicholson\",\n" +
                "    \"email\": \"jacknich@example.com\",\n" +
                "    \"metadata\": { \"intelligence\" : 7 },\n" +
                "    \"enabled\": true\n" +
                "  }\n" +
                "}";
        final GetUsersResponse response = GetUsersResponse.fromXContent((XContentType.JSON.xContent().createParser(
            new NamedXContentRegistry(Collections.emptyList()), new DeprecationHandler() {
                @Override
                public void usedDeprecatedName(String usedName, String modernName) {
                }

                @Override
                public void usedDeprecatedField(String usedName, String replacedWith) {
                }
            }, json)));
        assertThat(response.getUsers().size(), equalTo(1));
        final User user = response.getUsers().get(0);
        assertThat(user.getUsername(), equalTo("jacknich"));
        assertThat(user.getRoles().size(), equalTo(2));
        assertThat(user.getFullName(), equalTo("Jack Nicholson"));
        assertThat(user.getEmail(), equalTo("jacknich@example.com"));
        assertTrue(user.getEnabled());
        final Map<String, Object> metadata = new HashMap<>();
        metadata.put("intelligence", 7);
        assertThat(metadata, equalTo(user.getMetadata()));
    }

    public void testEqualsHashCode() {
        final List<User> users = new ArrayList<>();
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("intelligence", 1);
        final User user1 = new User("testUser1", Arrays.asList(new String[] {"admin", "other_role1"}),
            metadata, true, "Test User 1", null);
        users.add(user1);
        Map<String, Object> metadata2 = new HashMap<>();
        metadata2.put("intelligence", 9);
        metadata2.put("specialty", "geo");
        final User user2 = new User("testUser2", Arrays.asList(new String[] {"admin"}),
            metadata, true, "Test User 2", "testuser2@example.com");
        users.add(user2);
        final GetUsersResponse getUsersResponse = new GetUsersResponse(users);
        assertNotNull(getUsersResponse);
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(getUsersResponse, (original) -> {
            return new GetUsersResponse(original.getUsers());
        });
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(getUsersResponse, (original) -> {
            return new GetUsersResponse(original.getUsers());
        }, GetUsersResponseTests::mutateTestItem);
    }

    private static GetUsersResponse mutateTestItem(GetUsersResponse original) {
        if (randomBoolean()) {
            final List<User> users = new ArrayList<>();
            Map<String, Object> metadata = new HashMap<>();
            metadata.put("intelligence", 1);
            final User user1 = new User("testUser1", Arrays.asList(new String[] {"admin", "other_role1"}),
                metadata, true, "Test User 1", null);
            users.add(user1);
            return new GetUsersResponse(users);
        }
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("intelligence", 5);  // change intelligence
        final User user1 = new User("testUser1", Arrays.asList(new String[] {"admin", "other_role1"}),
            metadata, true, "Test User 1", null);
        List<User> newUsers = original.getUsers().stream().collect(Collectors.toList());
        newUsers.remove(0);
        newUsers.add(user1);
        return new GetUsersResponse(newUsers);
    }
}

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
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;
import org.elasticsearch.test.XContentTestUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import static org.elasticsearch.client.security.GetUsersResponse.toMap;
import static org.elasticsearch.test.AbstractXContentTestCase.xContentTester;

/** tests the Response for getting users from the security HLRC */
public class GetUsersResponseTests extends ESTestCase {

    public void testFromXContent() throws IOException {
        xContentTester(this::createParser,
            GetUsersResponseTests::createTestInstance,
            this::toXContent,
            GetUsersResponse::fromXContent)
            .supportsUnknownFields(false)
            .assertToXContentEquivalence(false)
            .test();
    }

    private XContentBuilder toXContentUser(User user, boolean enabled, XContentBuilder builder) throws IOException {
        XContentBuilder tempBuilder = JsonXContent.contentBuilder();
        tempBuilder.startObject();
        tempBuilder.field("username", user.getUsername());
        tempBuilder.array("roles", user.getRoles().toArray());
        tempBuilder.field("full_name", user.getFullName());
        tempBuilder.field("email", user.getEmail());
        tempBuilder.field("metadata", user.getMetadata());
        tempBuilder.field("enabled", enabled);
        tempBuilder.endObject();

        // This sub object should support unknown fields, but metadata cannot contain complex extra objects or it will fail
        Predicate<String> excludeFilter = path -> path.equals("metadata");
        BytesReference newBytes = XContentTestUtils.insertRandomFields(XContentType.JSON, BytesReference.bytes(tempBuilder),
            excludeFilter, random());
        builder.rawValue(newBytes.streamInput(), XContentType.JSON);
        return builder;
    }

    private XContentBuilder toXContent(GetUsersResponse response, XContentBuilder builder) throws IOException {
        builder.startObject();

        List<User> disabledUsers = new ArrayList<>(response.getUsers());
        disabledUsers.removeAll(response.getEnabledUsers());

        for (User user : disabledUsers) {
            builder.field(user.getUsername());
            toXContentUser(user, false, builder);
        }
        for (User user : response.getEnabledUsers()) {
            builder.field(user.getUsername());
            toXContentUser(user, true, builder);
        }
        builder.endObject();
        return builder;
    }

    private static GetUsersResponse createTestInstance() {
        final List<User> users = new ArrayList<>();
        final List<User> enabledUsers = new ArrayList<>();
        Map<String, Object> metadata = new HashMap<>();
        metadata.put(randomAlphaOfLengthBetween(1, 5), randomInt());

        final User user1 = new User(randomAlphaOfLength(8),
            Arrays.asList(new String[] {randomAlphaOfLength(5), randomAlphaOfLength(5)}),
            metadata, randomAlphaOfLength(10), null);
        users.add(user1);
        enabledUsers.add(user1);
        Map<String, Object> metadata2 = new HashMap<>();
        metadata2.put(randomAlphaOfLengthBetween(1, 5), randomInt());
        metadata2.put(randomAlphaOfLengthBetween(1, 5), randomBoolean());

        final User user2 = new User(randomAlphaOfLength(8),
            Arrays.asList(new String[] {randomAlphaOfLength(5), randomAlphaOfLength(5)}),
            metadata2, randomAlphaOfLength(10), null);
        users.add(user2);
        return new GetUsersResponse(toMap(users), toMap(enabledUsers));
    }

    public void testEqualsHashCode() {
        final List<User> users = new ArrayList<>();
        final List<User> enabledUsers = new ArrayList<>();
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("intelligence", 1);
        final User user1 = new User("testUser1", Arrays.asList(new String[] {"admin", "other_role1"}),
            metadata, "Test User 1", null);
        users.add(user1);
        enabledUsers.add(user1);
        Map<String, Object> metadata2 = new HashMap<>();
        metadata2.put("intelligence", 9);
        metadata2.put("specialty", "geo");
        final User user2 = new User("testUser2", Arrays.asList(new String[] {"admin"}),
            metadata2, "Test User 2", "testuser2@example.com");
        users.add(user2);
        enabledUsers.add(user2);
        final GetUsersResponse getUsersResponse = new GetUsersResponse(toMap(users), toMap(enabledUsers));
        assertNotNull(getUsersResponse);
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(
                getUsersResponse,
                (original) -> new GetUsersResponse(toMap(original.getUsers()), toMap(original.getEnabledUsers())));
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(
                getUsersResponse,
                (original) -> new GetUsersResponse(toMap(original.getUsers()), toMap(original.getEnabledUsers())),
                GetUsersResponseTests::mutateTestItem);
    }

    private static GetUsersResponse mutateTestItem(GetUsersResponse original) {
        if (randomBoolean()) {
            final List<User> users = new ArrayList<>();
            final List<User> enabledUsers = new ArrayList<>();
            Map<String, Object> metadata = new HashMap<>();
            metadata.put("intelligence", 1);
            final User user1 = new User("testUser1", Arrays.asList(new String[] {"admin", "other_role1"}),
                metadata, "Test User 1", null);
            users.add(user1);
            enabledUsers.add(user1);
            return new GetUsersResponse(toMap(users), toMap(enabledUsers));
        }
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("intelligence", 5);  // change intelligence
        final User user1 = new User("testUser1", Arrays.asList(new String[] {"admin", "other_role1"}),
            metadata, "Test User 1", null);
        List<User> newUsers = new ArrayList<>(original.getUsers());
        List<User> enabledUsers = new ArrayList<>(original.getEnabledUsers());
        newUsers.clear();
        enabledUsers.clear();
        newUsers.add(user1);
        enabledUsers.add(user1);
        return new GetUsersResponse(toMap(newUsers), toMap(enabledUsers));
    }

}

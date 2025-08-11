/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.security;

import org.elasticsearch.client.security.user.User;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;
import org.elasticsearch.test.XContentTestUtils;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.elasticsearch.test.AbstractXContentTestCase.xContentTester;

/** tests the Response for getting users from the security HLRC */
public class GetUsersResponseTests extends ESTestCase {

    public void testFromXContent() throws IOException {
        xContentTester(this::createParser, GetUsersResponseTests::createTestInstance, this::toXContent, GetUsersResponse::fromXContent)
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
        BytesReference newBytes = XContentTestUtils.insertRandomFields(
            XContentType.JSON,
            BytesReference.bytes(tempBuilder),
            excludeFilter,
            random()
        );
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
        final Set<User> users = new HashSet<>();
        final Set<User> enabledUsers = new HashSet<>();
        Map<String, Object> metadata = new HashMap<>();
        metadata.put(randomAlphaOfLengthBetween(1, 5), randomInt());

        final User user1 = new User(
            randomAlphaOfLength(8),
            Arrays.asList(new String[] { randomAlphaOfLength(5), randomAlphaOfLength(5) }),
            metadata,
            randomAlphaOfLength(10),
            null
        );
        users.add(user1);
        enabledUsers.add(user1);
        Map<String, Object> metadata2 = new HashMap<>();
        metadata2.put(randomAlphaOfLengthBetween(1, 5), randomInt());
        metadata2.put(randomAlphaOfLengthBetween(1, 5), randomBoolean());

        final User user2 = new User(
            randomAlphaOfLength(8),
            Arrays.asList(new String[] { randomAlphaOfLength(5), randomAlphaOfLength(5) }),
            metadata2,
            randomAlphaOfLength(10),
            null
        );
        users.add(user2);
        return new GetUsersResponse(users, enabledUsers);
    }

    public void testEqualsHashCode() {
        final Set<User> users = new HashSet<>();
        final Set<User> enabledUsers = new HashSet<>();
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("intelligence", 1);
        final User user1 = new User("testUser1", Arrays.asList(new String[] { "admin", "other_role1" }), metadata, "Test User 1", null);
        users.add(user1);
        enabledUsers.add(user1);
        Map<String, Object> metadata2 = new HashMap<>();
        metadata2.put("intelligence", 9);
        metadata2.put("specialty", "geo");
        final User user2 = new User("testUser2", Arrays.asList(new String[] { "admin" }), metadata, "Test User 2", "testuser2@example.com");
        users.add(user2);
        enabledUsers.add(user2);
        final GetUsersResponse getUsersResponse = new GetUsersResponse(users, enabledUsers);
        assertNotNull(getUsersResponse);
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(getUsersResponse, (original) -> {
            return new GetUsersResponse(original.getUsers(), original.getEnabledUsers());
        });
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(getUsersResponse, (original) -> {
            return new GetUsersResponse(original.getUsers(), original.getEnabledUsers());
        }, GetUsersResponseTests::mutateTestItem);
    }

    private static GetUsersResponse mutateTestItem(GetUsersResponse original) {
        if (randomBoolean()) {
            final Set<User> users = new HashSet<>();
            final Set<User> enabledUsers = new HashSet<>();
            Map<String, Object> metadata = new HashMap<>();
            metadata.put("intelligence", 1);
            final User user1 = new User("testUser1", Arrays.asList(new String[] { "admin", "other_role1" }), metadata, "Test User 1", null);
            users.add(user1);
            enabledUsers.add(user1);
            return new GetUsersResponse(users, enabledUsers);
        }
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("intelligence", 5);  // change intelligence
        final User user1 = new User("testUser1", Arrays.asList(new String[] { "admin", "other_role1" }), metadata, "Test User 1", null);
        Set<User> newUsers = original.getUsers().stream().collect(Collectors.toSet());
        Set<User> enabledUsers = original.getEnabledUsers().stream().collect(Collectors.toSet());
        newUsers.clear();
        enabledUsers.clear();
        newUsers.add(user1);
        enabledUsers.add(user1);
        return new GetUsersResponse(newUsers, enabledUsers);
    }
}

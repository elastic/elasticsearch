/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security;

import org.apache.http.HttpHeaders;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Strings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.user.User;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.core.IsEqual.equalTo;

public class QueryUserIT extends SecurityInBasicRestTestCase {

    private static final String READ_SECURITY_USER_AUTH_HEADER = "Basic cmVhZF9zZWN1cml0eV91c2VyOnJlYWQtc2VjdXJpdHktcGFzc3dvcmQ=";
    private static final String TEST_USER_NO_READ_USERS_AUTH_HEADER = "Basic c2VjdXJpdHlfdGVzdF91c2VyOnNlY3VyaXR5LXRlc3QtcGFzc3dvcmQ=";

    private static final Set<String> reservedUsers = Set.of(
        "elastic",
        "kibana",
        "kibana_system",
        "logstash_system",
        "beats_system",
        "apm_system",
        "remote_monitoring_user"
    );

    private Request queryUserRequestWithAuth() {
        return queryUserRequestWithAuth(false);
    }

    private Request queryUserRequestWithAuth(boolean withProfileId) {
        final Request request = new Request(
            randomFrom("POST", "GET"),
            "/_security/_query/user" + (withProfileId ? "?with_profile_uid=true" : randomFrom("", "?with_profile_uid=false"))
        );
        request.setOptions(request.getOptions().toBuilder().addHeader(HttpHeaders.AUTHORIZATION, READ_SECURITY_USER_AUTH_HEADER));
        return request;
    }

    public void testQuery() throws IOException {
        boolean withProfileId = randomBoolean();
        // No users to match yet
        assertQuery("", users -> assertThat(users, empty()), withProfileId);
        int randomUserCount = createRandomUsers().size();

        // An empty request body means search for all users (page size = 10)
        assertQuery("", users -> assertThat(users.size(), equalTo(Math.min(randomUserCount, 10))));

        // Match all
        assertQuery(
            String.format("""
                {"query":{"match_all":{}},"from":0,"size":%s}""", randomUserCount),
            users -> assertThat(users.size(), equalTo(randomUserCount)),
            withProfileId
        );

        // Exists query
        String field = randomFrom("username", "full_name", "roles", "enabled");
        assertQuery(
            String.format("""
                {"query":{"exists":{"field":"%s"}},"from":0,"size":%s}""", field, randomUserCount),
            users -> assertEquals(users.size(), randomUserCount),
            withProfileId
        );

        // Prefix search
        User prefixUser1 = createUser(
            "mr-prefix1",
            new String[] { "master-of-the-universe", "some-other-role" },
            "Prefix1",
            "email@something.com",
            Map.of(),
            true
        );
        User prefixUser2 = createUser(
            "mr-prefix2",
            new String[] { "master-of-the-world", "some-other-role" },
            "Prefix2",
            "email@something.com",
            Map.of(),
            true
        );
        // Extract map to be able to assert on profile id (not part of User model)
        Map<String, Object> prefixUser1Map;
        Map<String, Object> prefixUser2Map;
        if (withProfileId) {
            prefixUser1Map = userToMap(prefixUser1, doActivateProfile(prefixUser1.principal(), "100%-security-guaranteed"));
            prefixUser2Map = userToMap(prefixUser2, doActivateProfile(prefixUser2.principal(), "100%-security-guaranteed"));
            assertTrue(prefixUser1Map.containsKey("profile_uid"));
            assertTrue(prefixUser2Map.containsKey("profile_uid"));
        } else {
            prefixUser1Map = userToMap(prefixUser1);
            prefixUser2Map = userToMap(prefixUser2);
        }

        assertQuery("""
            {"query":{"bool":{"must":[{"prefix":{"roles":"master-of-the"}}]}},"sort":["username"]}""", returnedUsers -> {
            assertThat(returnedUsers, hasSize(2));
            assertUser(prefixUser1Map, returnedUsers.get(0));
            assertUser(prefixUser2Map, returnedUsers.get(1));
        }, withProfileId);

        // Wildcard search
        assertQuery("""
            { "query": { "wildcard": {"username": "mr-prefix*"} },"sort":["username"]}""", users -> {
            assertThat(users.size(), equalTo(2));
            assertUser(prefixUser1Map, users.get(0));
            assertUser(prefixUser2Map, users.get(1));
        }, withProfileId);

        // Terms query
        assertQuery("""
            {"query":{"terms":{"roles":["some-other-role"]}},"sort":["username"]}""", users -> {
            assertThat(users.size(), equalTo(2));
            assertUser(prefixUser1Map, users.get(0));
            assertUser(prefixUser2Map, users.get(1));
        }, withProfileId);

        // Test other fields
        User otherFieldsTestUser = createUser(
            "batman-official-user",
            new String[] { "bat-cave-admin" },
            "Batman",
            "batman@hotmail.com",
            Map.of(),
            true
        );
        String enabledTerm = "\"enabled\":true";
        String fullNameTerm = "\"full_name\":\"batman\"";
        String emailTerm = "\"email\":\"batman@hotmail.com\"";

        final String term = randomFrom(enabledTerm, fullNameTerm, emailTerm);
        assertQuery(
            Strings.format("""
                {"query":{"term":{%s}},"size":100}""", term),
            users -> assertThat(
                users.stream().map(u -> u.get(User.Fields.USERNAME.getPreferredName()).toString()).toList(),
                hasItem("batman-official-user")
            ),
            withProfileId
        );
        Map<String, Object> otherFieldsTestUserMap;
        if (withProfileId) {
            otherFieldsTestUserMap = userToMap(
                otherFieldsTestUser,
                doActivateProfile(otherFieldsTestUser.principal(), "100%-security-guaranteed")
            );
            assertTrue(otherFieldsTestUserMap.containsKey("profile_uid"));
        } else {
            otherFieldsTestUserMap = userToMap(otherFieldsTestUser);
        }
        // Test complex query
        assertQuery("""
            { "query": {"bool": {"must": [
            {"wildcard": {"username": "batman-official*"}},
            {"term": {"enabled": true}}],"filter": [{"prefix": {"roles": "bat-cave"}}]}}}""", users -> {
            assertThat(users.size(), equalTo(1));
            assertUser(otherFieldsTestUserMap, users.get(0));
        }, withProfileId);

        // Search for fields outside the allowlist fails
        assertQueryError(400, """
            { "query": { "prefix": {"not_allowed": "ABC"} } }""");

        // Search for fields that are not allowed in Query DSL but used internally by the service itself
        final String fieldName = randomFrom("type", "password");
        assertQueryError(400, Strings.format("""
            { "query": { "term": {"%s": "%s"} } }""", fieldName, randomAlphaOfLengthBetween(3, 8)));

        // User without read_security gets 403 trying to search Users
        assertQueryError(TEST_USER_NO_READ_USERS_AUTH_HEADER, 403, """
            { "query": { "wildcard": {"name": "*prefix*"} } }""");

        // Span term query not supported
        assertQueryError(400, """
            {"query":{"span_term":{"username": "X"} } }""");

        // Fuzzy query not supported
        assertQueryError(400, """
            { "query": { "fuzzy": { "username": "X" } } }""");

        // Make sure we can't query reserved users
        String reservedUsername = getReservedUsernameAndAssertExists();
        assertQuery(String.format("""
            {"query":{"term":{"username":"%s"}}}""", reservedUsername), users -> assertTrue(users.isEmpty()));
    }

    public void testPagination() throws IOException {
        final List<User> users = createRandomUsers();

        final int from = randomIntBetween(0, 3);
        final int size = randomIntBetween(2, 5);
        final int remaining = users.size() - from;

        // Using string only sorting to simplify test
        final String sortField = "username";
        final List<Map<String, Object>> allUserInfos = new ArrayList<>(remaining);
        {
            Request request = queryUserRequestWithAuth();
            request.setJsonEntity("{\"from\":" + from + ",\"size\":" + size + ",\"sort\":[\"" + sortField + "\"]}");
            allUserInfos.addAll(collectUsers(request, users.size()));
        }
        // first batch should be a full page
        assertThat(allUserInfos.size(), equalTo(size));

        while (allUserInfos.size() < remaining) {
            final Request request = queryUserRequestWithAuth();
            final List<Object> sortValues = extractSortValues(allUserInfos.get(allUserInfos.size() - 1));

            request.setJsonEntity(Strings.format("""
                {"size":%s,"sort":["%s"],"search_after":["%s"]}
                """, size, sortField, sortValues.get(0)));
            final List<Map<String, Object>> userInfoPage = collectUsers(request, users.size());

            if (userInfoPage.isEmpty() && allUserInfos.size() < remaining) {
                fail("fail to retrieve all Users, expect [" + remaining + "], got [" + allUserInfos + "]");
            }
            allUserInfos.addAll(userInfoPage);

            // Before all users are retrieved, each page should be a full page
            if (allUserInfos.size() < remaining) {
                assertThat(userInfoPage.size(), equalTo(size));
            }
        }

        // Assert sort values match the field of User information
        assertThat(
            allUserInfos.stream().map(m -> m.get(sortField)).toList(),
            equalTo(allUserInfos.stream().map(m -> extractSortValues(m).get(0)).toList())
        );

        // Assert that all users match the created users and that they're sorted correctly
        assertUsers(users, allUserInfos, sortField, from);

        // size can be zero, but total should still reflect the number of users matched
        final Request request = queryUserRequestWithAuth(false);
        request.setJsonEntity("{\"size\":0}");
        final Response response = client().performRequest(request);
        assertOK(response);
        final Map<String, Object> responseMap = responseAsMap(response);
        assertThat(responseMap.get("total"), equalTo(users.size()));
        assertThat(responseMap.get("count"), equalTo(0));
    }

    @SuppressWarnings("unchecked")
    public void testSort() throws IOException {
        final List<User> testUsers = List.of(
            createUser("a", new String[] { "4", "5", "6" }),
            createUser("b", new String[] { "5", "6" }),
            createUser("c", new String[] { "7", "8" })
        );
        assertQuery("""
            {"sort":[{"username":{"order":"desc"}}]}""", users -> {
            assertThat(users.size(), equalTo(3));
            for (int i = 2, j = 0; i >= 0; i--, j++) {
                assertUser(testUsers.get(j), users.get(i));
                assertThat(users.get(i).get("username"), equalTo(((List<String>) users.get(i).get("_sort")).get(0)));
            }
        });

        assertQuery("""
            {"sort":[{"username":{"order":"asc"}}]}""", users -> {
            assertThat(users.size(), equalTo(3));
            for (int i = 0; i <= 2; i++) {
                assertUser(testUsers.get(i), users.get(i));
                assertThat(users.get(i).get("username"), equalTo(((List<String>) users.get(i).get("_sort")).get(0)));
            }
        });

        assertQuery("""
            {"sort":[{"roles":{"order":"asc"}}]}""", users -> {
            assertThat(users.size(), equalTo(3));
            for (int i = 0; i <= 2; i++) {
                assertUser(testUsers.get(i), users.get(i));
                // Only first element of array is used for sorting
                assertThat(((List<String>) users.get(i).get("roles")).get(0), equalTo(((List<String>) users.get(i).get("_sort")).get(0)));
            }
        });

        // Make sure sorting on _doc works
        assertQuery("""
            {"sort":["_doc"]}""", users -> assertThat(users.size(), equalTo(3)));

        // Make sure multi-field sorting works
        assertQuery("""
            {"sort":[{"username":{"order":"asc"}}, {"roles":{"order":"asc"}}]}""", users -> {
            assertThat(users.size(), equalTo(3));
            for (int i = 0; i <= 2; i++) {
                assertUser(testUsers.get(i), users.get(i));
                assertThat(users.get(i).get("username"), equalTo(((List<String>) users.get(i).get("_sort")).get(0)));
                assertThat(((List<String>) users.get(i).get("roles")).get(0), equalTo(((List<String>) users.get(i).get("_sort")).get(1)));
            }
        });

        final String invalidFieldName = randomFrom("doc_type", "invalid", "password");
        assertQueryError(400, "{\"sort\":[\"" + invalidFieldName + "\"]}");

        final String invalidSortName = randomFrom("email", "full_name");
        assertQueryError(
            READ_SECURITY_USER_AUTH_HEADER,
            400,
            Strings.format("{\"sort\":[\"%s\"]}", invalidSortName),
            Strings.format("sorting is not supported for field [%s]", invalidSortName)
        );
    }

    private String getReservedUsernameAndAssertExists() throws IOException {
        String username = randomFrom(reservedUsers);
        final Request request = new Request("GET", "/_security/user");

        if (randomBoolean()) {
            // Update the user to create it in the security index
            Request putUserRequest = new Request("PUT", "/_security/user/" + username);
            putUserRequest.setJsonEntity("{\"enabled\": true}");
        }

        request.setOptions(request.getOptions().toBuilder().addHeader(HttpHeaders.AUTHORIZATION, READ_SECURITY_USER_AUTH_HEADER));
        final Response response = client().performRequest(request);
        assertOK(response);
        final Map<String, Object> responseMap = responseAsMap(response);
        assertNotNull(responseMap.get(username));
        return username;
    }

    @SuppressWarnings("unchecked")
    private List<Object> extractSortValues(Map<String, Object> userInfo) {
        return (List<Object>) userInfo.get("_sort");
    }

    private List<Map<String, Object>> collectUsers(Request request, int total) throws IOException {
        final Response response = client().performRequest(request);
        assertOK(response);
        final Map<String, Object> responseMap = responseAsMap(response);
        @SuppressWarnings("unchecked")
        final List<Map<String, Object>> userInfos = (List<Map<String, Object>>) responseMap.get("users");
        assertThat(responseMap.get("total"), equalTo(total));
        assertThat(responseMap.get("count"), equalTo(userInfos.size()));
        return userInfos;
    }

    private void assertQueryError(int statusCode, String body) {
        assertQueryError(READ_SECURITY_USER_AUTH_HEADER, statusCode, body);
    }

    private void assertQueryError(String authHeader, int statusCode, String body) {
        assertQueryError(authHeader, statusCode, body, null);
    }

    private void assertQueryError(String authHeader, int statusCode, String body, String errorMessage) {
        final Request request = new Request(randomFrom("GET", "POST"), "/_security/_query/user");
        request.setJsonEntity(body);
        request.setOptions(request.getOptions().toBuilder().addHeader(HttpHeaders.AUTHORIZATION, authHeader));
        final ResponseException responseException = expectThrows(ResponseException.class, () -> client().performRequest(request));
        assertThat(responseException.getResponse().getStatusLine().getStatusCode(), equalTo(statusCode));
        if (errorMessage != null) {
            assertTrue(responseException.getMessage().contains(errorMessage));
        }
    }

    private void assertQuery(String body, Consumer<List<Map<String, Object>>> userVerifier) throws IOException {
        assertQuery(body, userVerifier, false);
    }

    private void assertQuery(String body, Consumer<List<Map<String, Object>>> userVerifier, boolean withProfileId) throws IOException {
        final Request request = queryUserRequestWithAuth(withProfileId);
        request.setJsonEntity(body);
        final Response response = client().performRequest(request);
        assertOK(response);
        final Map<String, Object> responseMap = responseAsMap(response);
        @SuppressWarnings("unchecked")
        final List<Map<String, Object>> users = (List<Map<String, Object>>) responseMap.get("users");
        userVerifier.accept(users);
    }

    private void assertUser(User expectedUser, Map<String, Object> actualUser) {
        assertUser(userToMap(expectedUser), actualUser);
    }

    @SuppressWarnings("unchecked")
    private void assertUser(Map<String, Object> expectedUser, Map<String, Object> actualUser) {
        assertEquals(expectedUser.get(User.Fields.USERNAME.getPreferredName()), actualUser.get(User.Fields.USERNAME.getPreferredName()));
        assertArrayEquals(
            ((List<String>) expectedUser.get(User.Fields.ROLES.getPreferredName())).toArray(),
            ((List<String>) actualUser.get(User.Fields.ROLES.getPreferredName())).toArray()
        );
        assertEquals(expectedUser.getOrDefault("profile_uid", null), actualUser.getOrDefault("profile_uid", null));

        assertEquals(expectedUser.get(User.Fields.FULL_NAME.getPreferredName()), actualUser.get(User.Fields.FULL_NAME.getPreferredName()));
        assertEquals(expectedUser.get(User.Fields.EMAIL.getPreferredName()), actualUser.get(User.Fields.EMAIL.getPreferredName()));
        assertEquals(expectedUser.get(User.Fields.METADATA.getPreferredName()), actualUser.get(User.Fields.METADATA.getPreferredName()));
        assertEquals(expectedUser.get(User.Fields.ENABLED.getPreferredName()), actualUser.get(User.Fields.ENABLED.getPreferredName()));
    }

    private Map<String, Object> userToMap(User user) {
        return userToMap(user, null);
    }

    private Map<String, Object> userToMap(User user, @Nullable String profileId) {
        Map<String, Object> userMap = new HashMap<>();
        userMap.put(User.Fields.USERNAME.getPreferredName(), user.principal());
        userMap.put(User.Fields.ROLES.getPreferredName(), Arrays.stream(user.roles()).toList());
        userMap.put(User.Fields.FULL_NAME.getPreferredName(), user.fullName());
        userMap.put(User.Fields.EMAIL.getPreferredName(), user.email());
        userMap.put(User.Fields.METADATA.getPreferredName(), user.metadata());
        userMap.put(User.Fields.ENABLED.getPreferredName(), user.enabled());
        if (profileId != null) {
            userMap.put("profile_uid", profileId);
        }
        return userMap;
    }

    private void assertUsers(List<User> expectedUsers, List<Map<String, Object>> actualUsers, String sortField, int from) {
        assertEquals(expectedUsers.size() - from, actualUsers.size());

        List<Map<String, Object>> sortedExpectedUsers = expectedUsers.stream()
            .map(this::userToMap)
            .sorted(Comparator.comparing(user -> user.get(sortField).toString()))
            .toList();

        for (int i = from; i < sortedExpectedUsers.size(); i++) {
            assertUser(sortedExpectedUsers.get(i), actualUsers.get(i - from));
        }
    }

    public static Map<String, Object> randomUserMetadata() {
        return ESTestCase.randomFrom(
            Map.of(
                "employee_id",
                ESTestCase.randomAlphaOfLength(5),
                "number",
                1,
                "numbers",
                List.of(1, 3, 5),
                "extra",
                Map.of("favorite pizza", "margherita", "age", 42)
            ),
            Map.of(ESTestCase.randomAlphaOfLengthBetween(3, 8), ESTestCase.randomAlphaOfLengthBetween(3, 8)),
            Map.of(),
            null
        );
    }

    private String doActivateProfile(String username, String password) {
        final Request activateProfileRequest = new Request("POST", "_security/profile/_activate");
        activateProfileRequest.setJsonEntity(org.elasticsearch.common.Strings.format("""
            {
              "grant_type": "password",
              "username": "%s",
              "password": "%s"
            }""", username, password));

        final Response activateProfileResponse;

        try {
            activateProfileResponse = adminClient().performRequest(activateProfileRequest);
            assertOK(activateProfileResponse);
            return responseAsMap(activateProfileResponse).get("uid").toString();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private List<User> createRandomUsers() throws IOException {
        int randomUserCount = randomIntBetween(8, 15);
        final List<User> users = new ArrayList<>(randomUserCount);

        for (int i = 0; i < randomUserCount; i++) {
            users.add(
                createUser(
                    randomValueOtherThanMany(reservedUsers::contains, () -> randomAlphaOfLengthBetween(3, 8)) + "-" + i,
                    randomArray(1, 3, String[]::new, () -> randomAlphaOfLengthBetween(3, 8)),
                    randomAlphaOfLengthBetween(3, 8),
                    randomAlphaOfLengthBetween(3, 8),
                    randomUserMetadata(),
                    randomBoolean()
                )
            );
        }

        return users;
    }

    private User createUser(String userName, String[] roles) throws IOException {
        return createUser(
            userName,
            roles,
            randomAlphaOfLengthBetween(3, 8),
            randomAlphaOfLengthBetween(3, 8),
            randomUserMetadata(),
            randomBoolean()
        );
    }

    private User createUser(String userName, String[] roles, String fullName, String email, Map<String, Object> metadata, boolean enabled)
        throws IOException {

        final Request request = new Request("POST", "/_security/user/" + userName);
        BytesReference source = BytesReference.bytes(
            jsonBuilder().map(
                Map.of(
                    User.Fields.USERNAME.getPreferredName(),
                    userName,
                    User.Fields.ROLES.getPreferredName(),
                    roles,
                    User.Fields.FULL_NAME.getPreferredName(),
                    fullName,
                    User.Fields.EMAIL.getPreferredName(),
                    email,
                    User.Fields.METADATA.getPreferredName(),
                    metadata == null ? Map.of() : metadata,
                    User.Fields.PASSWORD.getPreferredName(),
                    "100%-security-guaranteed",
                    User.Fields.ENABLED.getPreferredName(),
                    enabled
                )
            )
        );
        request.setJsonEntity(source.utf8ToString());
        Response response = adminClient().performRequest(request);
        assertOK(response);
        assertTrue((boolean) responseAsMap(response).get("created"));
        return new User(userName, roles, fullName, email, metadata, enabled);
    }
}

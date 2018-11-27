/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.qa.security;

import org.apache.http.HttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.NotEqualMessageBuilder;
import org.elasticsearch.test.rest.ESRestTestCase;

import java.io.IOException;
import java.io.InputStream;
import java.sql.JDBCType;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.sql.qa.rest.RestSqlTestCase.columnInfo;

public class UserFunctionIT extends ESRestTestCase {

    private static final String SQL = "SELECT USER()";
    // defined in roles.yml
    private static final String MINIMAL_ACCESS_ROLE = "rest_minimal";
    
    @Override
    protected Settings restClientSettings() {
        return RestSqlIT.securitySettings();
    }
    
    @Override
    protected String getProtocol() {
        return RestSqlIT.SSL_ENABLED ? "https" : "http";
    }
    
    public void testSingleRandomUser() throws IOException {
        String randomUserName = randomAlphaOfLengthBetween(1, 15);
        String mode = randomMode().toString();
        createUser(randomUserName, MINIMAL_ACCESS_ROLE);
        
        Map<String, Object> expected = new HashMap<>();
        expected.put("columns", Arrays.asList(
                columnInfo(mode, "USER", "keyword", JDBCType.VARCHAR, 0)));
        expected.put("rows", Arrays.asList(Arrays.asList(randomUserName)));
        Map<String, Object> actual = runSql(randomUserName, mode, SQL);
        
        assertResponse(expected, actual);
    }
    
    public void testMultipleRandomUsersAccess() throws IOException {
        // create a random number of users
        int usersCount = randomIntBetween(5,  15);
        Set<String> users = new HashSet<String>();
        for(int i = 0; i < usersCount; i++) {
            String randomUserName = randomAlphaOfLengthBetween(1, 15);
            users.add(randomUserName);
            createUser(randomUserName, MINIMAL_ACCESS_ROLE);
        }
        
        // run 30 queries and pick each time one of the 5-15 users created previously
        for (int i = 0; i < 30; i++) {
            String mode = randomMode().toString();
            String randomlyPickedUsername = randomFrom(users);
            Map<String, Object> expected = new HashMap<>();

            expected.put("columns", Arrays.asList(
                    columnInfo(mode, "USER", "keyword", JDBCType.VARCHAR, 0)));
            expected.put("rows", Arrays.asList(Arrays.asList(randomlyPickedUsername)));
            Map<String, Object> actual = runSql(randomlyPickedUsername, mode, SQL);
            
            // expect the user that ran the query to be the same as the one returned by the `USER()` function
            assertResponse(expected, actual);
        }
    }
    
    private void createUser(String name, String role) throws IOException {
        Request request = new Request("PUT", "/_xpack/security/user/" + name);
        XContentBuilder user = JsonXContent.contentBuilder().prettyPrint();
        user.startObject(); {
            user.field("password", "testpass");
            user.field("roles", role);
        }
        user.endObject();
        request.setJsonEntity(Strings.toString(user));
        client().performRequest(request);
    }
    
    private Map<String, Object> runSql(@Nullable String asUser, String mode, String sql) throws IOException {
        return runSql(asUser, mode, new StringEntity("{\"query\": \"" + sql + "\"}", ContentType.APPLICATION_JSON));
    }
    
    private Map<String, Object> runSql(@Nullable String asUser, String mode, HttpEntity entity) throws IOException {
        Request request = new Request("POST", "/_xpack/sql");
        if (false == mode.isEmpty()) {
            request.addParameter("mode", mode);
        }
        if (asUser != null) {
            RequestOptions.Builder options = request.getOptions().toBuilder();
            options.addHeader("es-security-runas-user", asUser);
            request.setOptions(options);
        }
        request.setEntity(entity);
        return toMap(client().performRequest(request));
    }
    
    private void assertResponse(Map<String, Object> expected, Map<String, Object> actual) {
        if (false == expected.equals(actual)) {
            NotEqualMessageBuilder message = new NotEqualMessageBuilder();
            message.compareMaps(actual, expected);
            fail("Response does not match:\n" + message.toString());
        }
    }
    
    private static Map<String, Object> toMap(Response response) throws IOException {
        try (InputStream content = response.getEntity().getContent()) {
            return XContentHelper.convertToMap(JsonXContent.jsonXContent, content, false);
        }
    }

    private String randomMode() {
        return randomFrom("plain", "jdbc", "");
    }
}

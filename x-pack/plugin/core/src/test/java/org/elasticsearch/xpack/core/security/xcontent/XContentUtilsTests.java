/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.xcontent;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationField;
import org.elasticsearch.xpack.core.security.authc.AuthenticationTestHelper;
import org.elasticsearch.xpack.core.security.authc.AuthenticationTestHelper.AuthenticationTestBuilder;
import org.elasticsearch.xpack.core.security.user.User;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.core.security.authc.AuthenticationField.API_KEY_ID_KEY;
import static org.elasticsearch.xpack.core.security.authc.AuthenticationField.API_KEY_NAME_KEY;
import static org.elasticsearch.xpack.core.security.authc.AuthenticationField.CROSS_CLUSTER_ACCESS_AUTHENTICATION_KEY;
import static org.hamcrest.Matchers.equalTo;

public class XContentUtilsTests extends ESTestCase {

    public void testAddAuthorizationInfoWithNoAuthHeader() throws IOException {
        String json = generateJson(null);
        assertThat(json, equalTo("{}"));
        json = generateJson(Map.of());
        assertThat(json, equalTo("{}"));
        json = generateJson(Map.of(randomAlphaOfLengthBetween(10, 20), randomAlphaOfLengthBetween(20, 30)));
        assertThat(json, equalTo("{}"));
    }

    public void testAddAuthorizationInfoWithRoles() throws IOException {
        String[] roles = generateRandomStringArray(4, randomIntBetween(5, 15), false, false);
        User user = new User(randomAlphaOfLengthBetween(5, 15), roles);
        AuthenticationTestBuilder builder = AuthenticationTestHelper.builder().realm().user(user);
        Authentication authentication = builder.build();
        String json = generateJson(Map.of(AuthenticationField.AUTHENTICATION_KEY, authentication.encode()));
        assertThat(
            json,
            equalTo("{\"authorization\":{\"roles\":" + Arrays.stream(roles).collect(Collectors.joining("\",\"", "[\"", "\"]")) + "}}")
        );
    }

    public void testAddAuthorizationInfoWithApiKey() throws IOException {
        String apiKeyId = randomAlphaOfLength(20);
        String apiKeyName = randomAlphaOfLengthBetween(1, 16);
        AuthenticationTestBuilder builder = AuthenticationTestHelper.builder()
            .apiKey(apiKeyId)
            .metadata(Map.of(API_KEY_NAME_KEY, apiKeyName));
        Authentication authentication = builder.build();
        String json = generateJson(Map.of(AuthenticationField.AUTHENTICATION_KEY, authentication.encode()));
        assertThat(json, equalTo("{\"authorization\":{\"api_key\":{\"id\":\"" + apiKeyId + "\",\"name\":\"" + apiKeyName + "\"}}}"));
    }

    public void testAddAuthorizationInfoWithServiceAccount() throws IOException {
        String account = "elastic/" + randomFrom("kibana", "fleet-server", "enterprise-search-server");
        User user = new User(account);
        AuthenticationTestBuilder builder = AuthenticationTestHelper.builder().serviceAccount(user);
        Authentication authentication = builder.build();
        String json = generateJson(Map.of(AuthenticationField.AUTHENTICATION_KEY, authentication.encode()));
        assertThat(json, equalTo("{\"authorization\":{\"service_account\":\"" + account + "\"}}"));
    }

    public void testAddAuthorizationInfoWithCrossClusterAccess() throws IOException {
        final Authentication authentication = AuthenticationTestHelper.builder().crossClusterAccess().build();
        final var apiKeyName = (String) authentication.getAuthenticatingSubject().getMetadata().get(API_KEY_NAME_KEY);
        final var innerAuthentication = (Authentication) authentication.getAuthenticatingSubject()
            .getMetadata()
            .get(CROSS_CLUSTER_ACCESS_AUTHENTICATION_KEY);

        // Rely on the target function itself to generate the json string for inner authentication.
        // This is OK because other subject variants are tested elsewhere. We are only interested in the cross cluster variant here.
        String innerAuthenticationString = generateJson(Map.of(AuthenticationField.AUTHENTICATION_KEY, innerAuthentication.encode()));
        innerAuthenticationString = innerAuthenticationString.replace("{\"authorization\":", "");
        innerAuthenticationString = innerAuthenticationString.substring(0, innerAuthenticationString.length() - 1);

        String json = generateJson(Map.of(AuthenticationField.AUTHENTICATION_KEY, authentication.encode()));
        assertThat(
            json,
            equalTo(
                XContentHelper.stripWhitespace(
                    Strings.format(
                        """
                            {
                              "authorization": {
                                "cross_cluster_access": {
                                  "api_key": {
                                    "id": "%s"%s
                                  },
                                  "remote_authorization": %s
                                }
                              }
                            }""",
                        authentication.getAuthenticatingSubject().getMetadata().get(API_KEY_ID_KEY),
                        apiKeyName == null ? "" : ",\"name\":\"" + apiKeyName + "\"",
                        innerAuthenticationString
                    )
                )
            )
        );
    }

    public void testAddAuthorizationInfoWithCorruptData() throws IOException {
        String json = generateJson(Map.of(AuthenticationField.AUTHENTICATION_KEY, "corrupt"));
        assertThat(json, equalTo("{}"));
    }

    private String generateJson(Map<String, String> headers) throws IOException {
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            builder.startObject();
            XContentUtils.addAuthorizationInfo(builder, headers);
            builder.endObject();
            return Strings.toString(builder);
        }
    }
}

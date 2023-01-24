/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc;

import org.apache.http.client.methods.HttpPost;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.WarningsHandler;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken;
import org.elasticsearch.xpack.core.security.test.TestRestrictedIndices;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Integration Rest Test for testing authentication when all possible realms are configured
 */
public class FileRealmAuthIT extends SecurityRealmSmokeTestCase {

    // Declared in build.gradle
    private static final String USERNAME = "security_test_user";
    private static final String ANOTHER_USERNAME = "index_and_app_user";
    private static final SecureString PASSWORD = new SecureString("security-test-password".toCharArray());
    private static final String ROLE_NAME = "security_test_role";

    public void testAuthenticationUsingFileRealm() throws IOException {
        Map<String, Object> authenticate = super.authenticate(
            RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", UsernamePasswordToken.basicAuthHeaderValue(USERNAME, PASSWORD))
        );

        assertUsername(authenticate, USERNAME);
        assertRealm(authenticate, "file", "file0");
        assertRoles(authenticate, ROLE_NAME);
        assertNoApiKeyInfo(authenticate, Authentication.AuthenticationType.REALM);
    }

    public void testAuthenticationUsingFileRealmAndNoSecurityIndex() throws IOException {
        Map<String, Object> authenticate = super.authenticate(
            RequestOptions.DEFAULT.toBuilder()
                .addHeader("Authorization", UsernamePasswordToken.basicAuthHeaderValue(ANOTHER_USERNAME, PASSWORD))
        );

        try {
            // create user to ensure the .security-7 index exists
            createUser("dummy", new SecureString("longpassword".toCharArray()), List.of("whatever"));
            // close the .security-7 to simulate making it unavailable
            Request closeRequest = new Request(HttpPost.METHOD_NAME, TestRestrictedIndices.INTERNAL_SECURITY_MAIN_INDEX_7 + "/_close");
            closeRequest.setOptions(
                RequestOptions.DEFAULT.toBuilder()
                    .addHeader("Authorization", UsernamePasswordToken.basicAuthHeaderValue(ANOTHER_USERNAME, PASSWORD))
                    .setWarningsHandler(WarningsHandler.PERMISSIVE)
            );
            assertOK(client().performRequest(closeRequest));
            // clear the authentication cache
            Request clearCachesRequest = new Request(HttpPost.METHOD_NAME, "_security/realm/*/_clear_cache");
            clearCachesRequest.setOptions(
                RequestOptions.DEFAULT.toBuilder()
                    .addHeader("Authorization", UsernamePasswordToken.basicAuthHeaderValue(ANOTHER_USERNAME, PASSWORD))
            );
            assertOK(client().performRequest(clearCachesRequest));

            // file-realm authentication still works when cache is cleared and .security-7 is out
            assertUsername(authenticate, ANOTHER_USERNAME);
            assertRealm(authenticate, "file", "file0");
            assertRoles(authenticate, "all_index_privileges", "all_application_privileges");
            assertNoApiKeyInfo(authenticate, Authentication.AuthenticationType.REALM);
        } finally {
            Request openRequest = new Request(HttpPost.METHOD_NAME, TestRestrictedIndices.INTERNAL_SECURITY_MAIN_INDEX_7 + "/_open");
            openRequest.setOptions(
                RequestOptions.DEFAULT.toBuilder()
                    .addHeader("Authorization", UsernamePasswordToken.basicAuthHeaderValue(ANOTHER_USERNAME, PASSWORD))
                    .setWarningsHandler(WarningsHandler.PERMISSIVE)
            );
            assertOK(client().performRequest(openRequest));
            deleteUser("dummy");
        }
    }
}

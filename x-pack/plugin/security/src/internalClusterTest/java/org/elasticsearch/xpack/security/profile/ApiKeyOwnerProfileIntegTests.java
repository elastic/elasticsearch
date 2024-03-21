/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.profile;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.action.apikey.ApiKey;
import org.elasticsearch.xpack.core.security.action.apikey.CreateApiKeyAction;
import org.elasticsearch.xpack.core.security.action.apikey.CreateApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.profile.Profile;
import org.elasticsearch.xpack.core.security.action.user.PutUserAction;
import org.elasticsearch.xpack.core.security.action.user.PutUserRequest;
import org.elasticsearch.xpack.security.authc.ApiKeyIntegTests;
import org.junit.Before;

import java.util.Map;

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.WAIT_UNTIL;
import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.hamcrest.Matchers.is;

public class ApiKeyOwnerProfileIntegTests extends SecurityIntegTestCase {

    public static final SecureString FILE_USER_TEST_PASSWORD = new SecureString("file-user-test-password".toCharArray());
    public static final SecureString NATIVE_USER_TEST_PASSWORD = new SecureString("native-user-test-password".toCharArray());

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        final Settings.Builder builder = Settings.builder().put(super.nodeSettings(nodeOrdinal, otherSettings));
        // Register both file and native realms under the same domain
        // builder.put("xpack.security.authc.domains.my_domain.realms", "file,index");
        builder.put(XPackSettings.API_KEY_SERVICE_ENABLED_SETTING.getKey(), true);
        builder.put(XPackSettings.TOKEN_SERVICE_ENABLED_SETTING.getKey(), true);
        return builder.build();
    }

    @Before
    public void createNativeUsers() {
        {
            PutUserRequest putUserRequest = new PutUserRequest();
            putUserRequest.username("user_with_manage_api_key_role");
            putUserRequest.roles("manage_api_key_role");
            putUserRequest.passwordHash(getFastStoredHashAlgoForTests().hash(NATIVE_USER_TEST_PASSWORD));
            assertThat(client().execute(PutUserAction.INSTANCE, putUserRequest).actionGet().created(), is(true));
        }
        {
            PutUserRequest putUserRequest = new PutUserRequest();
            putUserRequest.username("user_with_manage_own_api_key_role");
            putUserRequest.roles("manage_own_api_key_role");
            putUserRequest.passwordHash(getFastStoredHashAlgoForTests().hash(NATIVE_USER_TEST_PASSWORD));
            assertThat(client().execute(PutUserAction.INSTANCE, putUserRequest).actionGet().created(), is(true));
        }
    }

    @Override
    public String configRoles() {
        return super.configRoles() + """
            manage_api_key_role:
              cluster: ["manage_api_key"]
            manage_own_api_key_role:
              cluster: ["manage_own_api_key"]
            """;
    }

    @Override
    public String configUsers() {
        final String usersPasswdHashed = new String(getFastStoredHashAlgoForTests().hash(FILE_USER_TEST_PASSWORD));
        return super.configUsers()
            + "user_with_manage_api_key_role:"
            + usersPasswdHashed
            + "\n"
            + "user_with_manage_own_api_key_role:"
            + usersPasswdHashed
            + "\n";
    }

    @Override
    public String configUsersRoles() {
        return super.configUsersRoles() + """
            manage_api_key_role:user_with_manage_api_key_role
            manage_own_api_key_role:user_with_manage_own_api_key_role
            """;
    }

    public void testApiKeyOwnerProfileNoDomain() {
        String username = randomFrom("user_with_manage_own_api_key_role", "user_with_manage_api_key_role");
        SecureString password = randomFrom(FILE_USER_TEST_PASSWORD, NATIVE_USER_TEST_PASSWORD);
        Client client = client().filterWithHeader(Map.of("Authorization", basicAuthHeaderValue(username, password)));
        CreateApiKeyRequest request = new CreateApiKeyRequest("key1", null, null, null);
        request.setRefreshPolicy(randomFrom(IMMEDIATE, WAIT_UNTIL));
        final boolean firstActivateProfile = randomBoolean();
        Profile userWithManageOwnProfile = null;
        // activate profile, then create API key, or vice-versa
        if (firstActivateProfile) {
            userWithManageOwnProfile = AbstractProfileIntegTestCase.doActivateProfile(username, password);
        }
        String keyId1 = client.execute(CreateApiKeyAction.INSTANCE, request).actionGet().getId();
        if (false == firstActivateProfile) {
            userWithManageOwnProfile = AbstractProfileIntegTestCase.doActivateProfile(username, password);
        }
        Tuple<ApiKey, String> apiKeyWithProfileUid = ApiKeyIntegTests.getApiKeyInfoWithProfileUid(client, keyId1, randomBoolean());
        assertThat(apiKeyWithProfileUid.v1().getId(), is(keyId1));
        assertThat(apiKeyWithProfileUid.v2(), is(userWithManageOwnProfile.uid()));
    }
}

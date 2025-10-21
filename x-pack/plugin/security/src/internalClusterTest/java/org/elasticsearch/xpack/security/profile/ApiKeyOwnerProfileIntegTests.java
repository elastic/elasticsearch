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
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalTestCluster;
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

import java.util.List;
import java.util.Map;

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.WAIT_UNTIL;
import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.iterableWithSize;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = false)
public class ApiKeyOwnerProfileIntegTests extends SecurityIntegTestCase {

    public static final SecureString FILE_USER_TEST_PASSWORD = new SecureString("file-user-test-password".toCharArray());
    public static final SecureString NATIVE_USER_TEST_PASSWORD = new SecureString("native-user-test-password".toCharArray());

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        final Settings.Builder builder = Settings.builder().put(super.nodeSettings(nodeOrdinal, otherSettings));
        builder.put(XPackSettings.API_KEY_SERVICE_ENABLED_SETTING.getKey(), true);
        builder.put(XPackSettings.TOKEN_SERVICE_ENABLED_SETTING.getKey(), true);
        return builder.build();
    }

    @Before
    public void createNativeUsers() {
        ensureGreen();
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

    public void testApiKeyOwnerProfileWithoutDomain() {
        boolean ownKey = randomBoolean();
        final String username;
        if (ownKey) {
            username = "user_with_manage_own_api_key_role";
        } else {
            username = "user_with_manage_api_key_role";
        }
        SecureString password = randomFrom(FILE_USER_TEST_PASSWORD, NATIVE_USER_TEST_PASSWORD);
        Client client = client().filterWithHeader(Map.of("Authorization", basicAuthHeaderValue(username, password)));
        CreateApiKeyRequest request = new CreateApiKeyRequest("key1", null, null, null);
        request.setRefreshPolicy(randomFrom(IMMEDIATE, WAIT_UNTIL));
        // activate profile, then create API key, or vice-versa
        boolean firstActivateProfile = randomBoolean();
        Profile userWithManageOwnProfile = null;
        if (firstActivateProfile) {
            userWithManageOwnProfile = AbstractProfileIntegTestCase.doActivateProfile(username, password);
        }
        String keyId = client.execute(CreateApiKeyAction.INSTANCE, request).actionGet().getId();
        if (false == firstActivateProfile) {
            userWithManageOwnProfile = AbstractProfileIntegTestCase.doActivateProfile(username, password);
        }
        // assert key owner profile uid
        Tuple<ApiKey, String> apiKeyWithProfileUid = ApiKeyIntegTests.getApiKeyInfoWithProfileUid(client, keyId, ownKey || randomBoolean());
        assertThat(apiKeyWithProfileUid.v1().getId(), is(keyId));
        assertThat(apiKeyWithProfileUid.v2(), is(userWithManageOwnProfile.uid()));
        // manage_api_key user can similarly observe the API key with the profile uid
        assertAllKeysWithProfiles(new String[] { keyId }, new String[] { userWithManageOwnProfile.uid() });
    }

    public void testApiKeyOwnerJoinsDomain() throws Exception {
        // one user creates the API Key, the other activates the profile
        String username = randomFrom("user_with_manage_own_api_key_role", "user_with_manage_api_key_role");
        SecureString password1;
        SecureString password2;
        if (randomBoolean()) {
            password1 = FILE_USER_TEST_PASSWORD;
            password2 = NATIVE_USER_TEST_PASSWORD;
        } else {
            password1 = NATIVE_USER_TEST_PASSWORD;
            password2 = FILE_USER_TEST_PASSWORD;
        }
        // activate profile, then create API key, or vice-versa
        boolean firstActivateProfile = randomBoolean();
        Profile user2Profile = null;
        if (firstActivateProfile) {
            user2Profile = AbstractProfileIntegTestCase.doActivateProfile(username, password2);
        }
        Client client1 = client().filterWithHeader(Map.of("Authorization", basicAuthHeaderValue(username, password1)));
        CreateApiKeyRequest request = new CreateApiKeyRequest("key1", null, null, null);
        request.setRefreshPolicy(randomFrom(IMMEDIATE, WAIT_UNTIL));
        String keyId = client1.execute(CreateApiKeyAction.INSTANCE, request).actionGet().getId();
        if (false == firstActivateProfile) {
            user2Profile = AbstractProfileIntegTestCase.doActivateProfile(username, password2);
        }
        // assert key owner (username1) without profile uid
        Tuple<ApiKey, String> apiKeyWithProfileUid = ApiKeyIntegTests.getApiKeyInfoWithProfileUid(
            client1,
            keyId,
            username.equals("user_with_manage_own_api_key_role") || randomBoolean()
        );
        assertThat(apiKeyWithProfileUid.v1().getId(), is(keyId));
        // no profile for API Key owner
        assertThat(apiKeyWithProfileUid.v2(), nullValue());
        // manage all api keys user can similarly see the key WITHOUT the profile uid
        assertAllKeysWithProfiles(new String[] { keyId }, new String[] { null });
        // restart cluster nodes to add the 2 users to the same domain
        internalCluster().fullRestart(new InternalTestCluster.RestartCallback() {
            @Override
            public Settings onNodeStopped(String nodeName) {
                // Register both file and native realms under the same domain
                return Settings.builder().put("xpack.security.authc.domains.my_domain.realms", "file,index").build();
            }
        });
        ensureGreen();
        client1 = client().filterWithHeader(Map.of("Authorization", basicAuthHeaderValue(username, password1)));
        apiKeyWithProfileUid = ApiKeyIntegTests.getApiKeyInfoWithProfileUid(
            client1,
            keyId,
            username.equals("user_with_manage_own_api_key_role") || randomBoolean()
        );
        assertThat(apiKeyWithProfileUid.v1().getId(), is(keyId));
        // API key owner (username1) now has a profile uid
        assertThat(apiKeyWithProfileUid.v2(), is(user2Profile.uid()));
        // manage all api keys user can similarly see the key with the profile uid
        assertAllKeysWithProfiles(new String[] { keyId }, new String[] { user2Profile.uid() });
    }

    public void testApiKeyOwnerLeavesDomain() throws Exception {
        // put the 2 realms under the same domain
        internalCluster().fullRestart(new InternalTestCluster.RestartCallback() {
            @Override
            public Settings onNodeStopped(String nodeName) {
                // Register both file and native realms under the same domain
                return Settings.builder().put("xpack.security.authc.domains.file_and_index_domain.realms", "file,index").build();
            }
        });
        ensureGreen();

        // one user creates the API Key, the other activates the profile
        String username = randomFrom("user_with_manage_own_api_key_role", "user_with_manage_api_key_role");
        SecureString password1;
        SecureString password2;
        if (randomBoolean()) {
            password1 = FILE_USER_TEST_PASSWORD;
            password2 = NATIVE_USER_TEST_PASSWORD;
        } else {
            password1 = NATIVE_USER_TEST_PASSWORD;
            password2 = FILE_USER_TEST_PASSWORD;
        }
        // activate profile, then create API key, or vice-versa
        boolean firstActivateProfile = randomBoolean();
        Profile user2Profile = null;
        if (firstActivateProfile) {
            user2Profile = AbstractProfileIntegTestCase.doActivateProfile(username, password2);
        }
        Client client1 = client().filterWithHeader(Map.of("Authorization", basicAuthHeaderValue(username, password1)));
        CreateApiKeyRequest request = new CreateApiKeyRequest("key1", null, null, null);
        request.setRefreshPolicy(randomFrom(IMMEDIATE, WAIT_UNTIL));
        String keyId = client1.execute(CreateApiKeyAction.INSTANCE, request).actionGet().getId();
        if (false == firstActivateProfile) {
            user2Profile = AbstractProfileIntegTestCase.doActivateProfile(username, password2);
        }
        // assert the key owner (password1) has profile uid of password2
        Tuple<ApiKey, String> apiKeyWithProfileUid = ApiKeyIntegTests.getApiKeyInfoWithProfileUid(
            client1,
            keyId,
            username.equals("user_with_manage_own_api_key_role") || randomBoolean()
        );
        assertThat(apiKeyWithProfileUid.v1().getId(), is(keyId));
        // API Key owner (password1) has profile of password2
        assertThat(apiKeyWithProfileUid.v2(), is(user2Profile.uid()));
        // manage all api keys user can similarly see the key with the profile uid of username2
        assertAllKeysWithProfiles(new String[] { keyId }, new String[] { user2Profile.uid() });
        // the realms are not under the same domain anymore
        internalCluster().fullRestart(new InternalTestCluster.RestartCallback() {
            @Override
            public Settings onNodeStopped(String nodeName) {
                // Register both file and native realms under the same domain
                Settings.Builder settingsBuilder = Settings.builder();
                settingsBuilder.put("xpack.security.authc.domains.file_and_index_domain.realms", (String) null);
                if (randomBoolean()) {
                    settingsBuilder.put("xpack.security.authc.domains.file_domain.realms", "file");
                }
                if (randomBoolean()) {
                    settingsBuilder.put("xpack.security.authc.domains.index_domain.realms", "index");
                }
                return settingsBuilder.build();
            }
        });
        ensureGreen();
        // assert the key owner (username1) now has no profile
        client1 = client().filterWithHeader(Map.of("Authorization", basicAuthHeaderValue(username, password1)));
        apiKeyWithProfileUid = ApiKeyIntegTests.getApiKeyInfoWithProfileUid(
            client1,
            keyId,
            username.equals("user_with_manage_own_api_key_role") || randomBoolean()
        );
        assertThat(apiKeyWithProfileUid.v1().getId(), is(keyId));
        // no profile for API Key owner
        assertThat(apiKeyWithProfileUid.v2(), nullValue());
        // manage all api keys user can similarly see the key WITHOUT the profile uid
        assertAllKeysWithProfiles(new String[] { keyId }, new String[] { null });
        // but password1 can also activate its own profile now
        Profile user1Profile = AbstractProfileIntegTestCase.doActivateProfile(username, password1);
        assertThat(user1Profile.uid(), not(user2Profile.uid()));
        // which is reflected in the API key owner profile uid information
        apiKeyWithProfileUid = ApiKeyIntegTests.getApiKeyInfoWithProfileUid(
            client1,
            keyId,
            username.equals("user_with_manage_own_api_key_role") || randomBoolean()
        );
        assertThat(apiKeyWithProfileUid.v1().getId(), is(keyId));
        // no profile for API Key owner
        assertThat(apiKeyWithProfileUid.v2(), is(user1Profile.uid()));
        // manage all api keys user can similarly see the key with the profile uid of username1 now
        assertAllKeysWithProfiles(new String[] { keyId }, new String[] { user1Profile.uid() });
    }

    public void testDifferentKeyOwnersSameProfile() throws Exception {
        // put the 2 realms under the same domain
        internalCluster().fullRestart(new InternalTestCluster.RestartCallback() {
            @Override
            public Settings onNodeStopped(String nodeName) {
                // Register both file and native realms under the same domain
                return Settings.builder().put("xpack.security.authc.domains.one_domain.realms", "file,index").build();
            }
        });
        ensureGreen();
        String username = randomFrom("user_with_manage_own_api_key_role", "user_with_manage_api_key_role");
        SecureString password1;
        SecureString password2;
        boolean user1IsFile;
        if (randomBoolean()) {
            password1 = FILE_USER_TEST_PASSWORD;
            user1IsFile = true;
            password2 = NATIVE_USER_TEST_PASSWORD;
        } else {
            password1 = NATIVE_USER_TEST_PASSWORD;
            user1IsFile = false;
            password2 = FILE_USER_TEST_PASSWORD;
        }
        // activate the profile, then create the 2 keys, or vice-versa
        Profile user1Profile = null;
        Profile user2Profile = null;
        boolean firstActivateProfile1 = randomBoolean();
        if (firstActivateProfile1) {
            user1Profile = AbstractProfileIntegTestCase.doActivateProfile(username, password1);
        }
        boolean firstActivateProfile2 = randomBoolean();
        if (firstActivateProfile2) {
            user2Profile = AbstractProfileIntegTestCase.doActivateProfile(username, password2);
        }
        Client client1 = client().filterWithHeader(Map.of("Authorization", basicAuthHeaderValue(username, password1)));
        CreateApiKeyRequest request1 = new CreateApiKeyRequest("key1", null, null, null);
        request1.setRefreshPolicy(randomFrom(IMMEDIATE, WAIT_UNTIL));
        String key1Id = client1.execute(CreateApiKeyAction.INSTANCE, request1).actionGet().getId();
        Client client2 = client().filterWithHeader(Map.of("Authorization", basicAuthHeaderValue(username, password2)));
        CreateApiKeyRequest request2 = new CreateApiKeyRequest("key2", null, null, null);
        request2.setRefreshPolicy(randomFrom(IMMEDIATE, WAIT_UNTIL));
        String key2Id = client2.execute(CreateApiKeyAction.INSTANCE, request2).actionGet().getId();
        if (false == firstActivateProfile1) {
            user1Profile = AbstractProfileIntegTestCase.doActivateProfile(username, password1);
        }
        if (false == firstActivateProfile2) {
            user2Profile = AbstractProfileIntegTestCase.doActivateProfile(username, password2);
        }
        // there should only be a single profile, because both users are under the same domain
        assertThat(user1Profile.uid(), is(user2Profile.uid()));
        String profileUid = user1Profile.uid();
        // the 2 API keys should also show the one profile uid for the 2 owners
        Tuple<ApiKey, String> apiKeyWithProfileUid = ApiKeyIntegTests.getApiKeyInfoWithProfileUid(
            client1,
            key1Id,
            username.equals("user_with_manage_own_api_key_role") || randomBoolean()
        );
        assertThat(apiKeyWithProfileUid.v1().getId(), is(key1Id));
        if (user1IsFile) {
            assertThat(apiKeyWithProfileUid.v1().getRealm(), is("file"));
        } else {
            assertThat(apiKeyWithProfileUid.v1().getRealm(), is("index"));
        }
        assertThat(apiKeyWithProfileUid.v2(), is(profileUid));
        apiKeyWithProfileUid = ApiKeyIntegTests.getApiKeyInfoWithProfileUid(
            client2,
            key2Id,
            username.equals("user_with_manage_own_api_key_role") || randomBoolean()
        );
        assertThat(apiKeyWithProfileUid.v1().getId(), is(key2Id));
        if (user1IsFile) {
            assertThat(apiKeyWithProfileUid.v1().getRealm(), is("index"));
        } else {
            assertThat(apiKeyWithProfileUid.v1().getRealm(), is("file"));
        }
        assertThat(apiKeyWithProfileUid.v2(), is(profileUid));
        // manage all api keys user can similarly see the 2 keys with the same profile uid
        assertAllKeysWithProfiles(new String[] { key1Id, key2Id }, new String[] { profileUid, profileUid });
    }

    private void assertAllKeysWithProfiles(String[] keyIds, String[] profileUids) {
        assert keyIds.length == profileUids.length;
        Client client = client().filterWithHeader(
            Map.of(
                "Authorization",
                basicAuthHeaderValue("user_with_manage_api_key_role", randomFrom(FILE_USER_TEST_PASSWORD, NATIVE_USER_TEST_PASSWORD))
            )
        );
        List<Tuple<String, String>> allApiKeyIdsWithProfileUid = ApiKeyIntegTests.getAllApiKeyInfoWithProfileUid(client)
            .stream()
            .map(t -> new Tuple<>(t.v1().getId(), t.v2()))
            .toList();
        assertThat(allApiKeyIdsWithProfileUid, iterableWithSize(keyIds.length));
        for (int i = 0; i < keyIds.length; i++) {
            assertThat(allApiKeyIdsWithProfileUid, hasItem(new Tuple<>(keyIds[i], profileUids[i])));
        }
    }
}

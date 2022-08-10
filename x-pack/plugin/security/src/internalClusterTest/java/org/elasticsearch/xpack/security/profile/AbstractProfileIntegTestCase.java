/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.profile;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.xpack.core.security.action.profile.ActivateProfileAction;
import org.elasticsearch.xpack.core.security.action.profile.ActivateProfileRequest;
import org.elasticsearch.xpack.core.security.action.profile.ActivateProfileResponse;
import org.elasticsearch.xpack.core.security.action.profile.GetProfilesAction;
import org.elasticsearch.xpack.core.security.action.profile.GetProfilesRequest;
import org.elasticsearch.xpack.core.security.action.profile.GetProfilesResponse;
import org.elasticsearch.xpack.core.security.action.profile.Profile;
import org.elasticsearch.xpack.core.security.action.token.CreateTokenAction;
import org.elasticsearch.xpack.core.security.action.token.CreateTokenRequest;
import org.elasticsearch.xpack.core.security.action.token.CreateTokenResponse;
import org.elasticsearch.xpack.core.security.action.user.PutUserAction;
import org.elasticsearch.xpack.core.security.action.user.PutUserRequest;
import org.junit.Before;

import java.util.Set;

import static org.elasticsearch.test.SecuritySettingsSource.TEST_PASSWORD_HASHED;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public abstract class AbstractProfileIntegTestCase extends SecurityIntegTestCase {

    protected static final String RAC_USER_NAME = "rac-user";
    protected static final String OTHER_RAC_USER_NAME = "other-rac-user";
    protected static final String RAC_ROLE = "rac_role";
    protected static final String NATIVE_RAC_ROLE = "native_rac_role";
    protected static final SecureString NATIVE_RAC_USER_PASSWORD = new SecureString("native_rac_user_password".toCharArray());

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        final Settings.Builder builder = Settings.builder().put(super.nodeSettings(nodeOrdinal, otherSettings));
        builder.put("xpack.security.authc.token.enabled", "true");
        return builder.build();
    }

    @Before
    public void createNativeUsers() {
        final PutUserRequest putUserRequest1 = new PutUserRequest();
        putUserRequest1.username(RAC_USER_NAME);
        putUserRequest1.roles(RAC_ROLE, NATIVE_RAC_ROLE);
        final String nativeRacUserPasswordHash = new String(getFastStoredHashAlgoForTests().hash(NATIVE_RAC_USER_PASSWORD));
        putUserRequest1.passwordHash(nativeRacUserPasswordHash.toCharArray());
        putUserRequest1.email(RAC_USER_NAME + "@example.com");
        assertThat(client().execute(PutUserAction.INSTANCE, putUserRequest1).actionGet().created(), is(true));
    }

    @Override
    protected String configUsers() {
        return super.configUsers()
            + RAC_USER_NAME
            + ":"
            + TEST_PASSWORD_HASHED
            + "\n"
            + OTHER_RAC_USER_NAME
            + ":"
            + TEST_PASSWORD_HASHED
            + "\n";
    }

    @Override
    protected String configRoles() {
        return super.configRoles()
            + "\n"
            + RAC_ROLE
            + ":\n"
            + "  cluster:\n"
            + "    - 'manage_own_api_key'\n"
            + "    - 'manage_token'\n"
            + "    - 'manage_service_account'\n"
            + "    - 'monitor'\n"
            + "  applications:\n"
            + "    - application: 'app-1'\n"
            + "      privileges: ['read']\n"
            + "      resources: ['foo*']\n";
    }

    @Override
    protected String configUsersRoles() {
        return super.configUsersRoles() + RAC_ROLE + ":" + RAC_USER_NAME + "," + OTHER_RAC_USER_NAME + "\n";
    }

    protected Profile doActivateProfile(String username, SecureString password) {
        // User and its access token should be associated to the same profile
        return doActivateProfile(username, password, randomBoolean());
    }

    protected Profile doActivateProfile(String username, SecureString password, boolean useToken) {
        final ActivateProfileRequest activateProfileRequest = new ActivateProfileRequest();
        if (useToken) {
            final CreateTokenRequest createTokenRequest = new CreateTokenRequest("password", username, password.clone(), null, null, null);
            final CreateTokenResponse createTokenResponse = client().execute(CreateTokenAction.INSTANCE, createTokenRequest).actionGet();
            activateProfileRequest.getGrant().setType("access_token");
            activateProfileRequest.getGrant().setAccessToken(new SecureString(createTokenResponse.getTokenString().toCharArray()));
        } else {
            activateProfileRequest.getGrant().setType("password");
            activateProfileRequest.getGrant().setUsername(username);
            // clone the secureString because activate action closes it afterwards
            activateProfileRequest.getGrant().setPassword(password.clone());
        }

        final ActivateProfileResponse activateProfileResponse = client().execute(ActivateProfileAction.INSTANCE, activateProfileRequest)
            .actionGet();
        final Profile profile = activateProfileResponse.getProfile();
        assertThat(profile, notNullValue());
        assertThat(profile.user().username(), equalTo(username));
        assertThat(profile.applicationData(), anEmptyMap());
        return profile;
    }

    protected Profile getProfile(String uid, Set<String> dataKeys) {
        final GetProfilesResponse getProfilesResponse = client().execute(GetProfilesAction.INSTANCE, new GetProfilesRequest(uid, dataKeys))
            .actionGet();
        assertThat(getProfilesResponse.getProfiles(), hasSize(1));
        return getProfilesResponse.getProfiles().get(0);
    }

    protected <T> T getInstanceFromRandomNode(Class<T> clazz) {
        final String nodeName = randomFrom(internalCluster().getNodeNames());
        return internalCluster().getInstance(clazz, nodeName);
    }
}

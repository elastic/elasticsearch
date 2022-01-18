/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.profile;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.indices.get.GetIndexAction;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.SecuritySingleNodeTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.security.action.profile.ActivateProfileAction;
import org.elasticsearch.xpack.core.security.action.profile.ActivateProfileRequest;
import org.elasticsearch.xpack.core.security.action.profile.ActivateProfileResponse;
import org.elasticsearch.xpack.core.security.action.profile.GetProfileAction;
import org.elasticsearch.xpack.core.security.action.profile.GetProfileRequest;
import org.elasticsearch.xpack.core.security.action.profile.GetProfilesResponse;
import org.elasticsearch.xpack.core.security.action.profile.Profile;
import org.elasticsearch.xpack.core.security.action.user.PutUserAction;
import org.elasticsearch.xpack.core.security.action.user.PutUserRequest;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.user.User;

import java.time.Instant;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.test.SecuritySettingsSource.TEST_PASSWORD_HASHED;
import static org.elasticsearch.test.SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING;
import static org.elasticsearch.xpack.security.support.SecuritySystemIndices.INTERNAL_SECURITY_PROFILE_INDEX_8;
import static org.elasticsearch.xpack.security.support.SecuritySystemIndices.SECURITY_PROFILE_ALIAS;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class ProfileSingleNodeTests extends SecuritySingleNodeTestCase {

    private static final String RAC_USER_NAME = "rac_user";

    @Override
    protected String configUsers() {
        return super.configUsers() + RAC_USER_NAME + ":" + TEST_PASSWORD_HASHED + "\n";
    }

    @Override
    protected String configRoles() {
        return super.configRoles() + "rac_role:\n" + "  cluster:\n" + "    - 'manage_own_api_key'\n" + "    - 'monitor'\n";
    }

    @Override
    protected String configUsersRoles() {
        return super.configUsersRoles() + "rac_role:" + RAC_USER_NAME + "\n";
    }

    public void testProfileIndexAutoCreation() {
        var indexResponse = client().prepareIndex(randomFrom(INTERNAL_SECURITY_PROFILE_INDEX_8, SECURITY_PROFILE_ALIAS))
            .setSource(Map.of("user_profile", Map.of("uid", randomAlphaOfLength(22))))
            .get();

        assertThat(indexResponse.status().getStatus(), equalTo(201));

        var getIndexRequest = new GetIndexRequest();
        getIndexRequest.indices(INTERNAL_SECURITY_PROFILE_INDEX_8);

        var getIndexResponse = client().execute(GetIndexAction.INSTANCE, getIndexRequest).actionGet();

        assertThat(getIndexResponse.getIndices(), arrayContaining(INTERNAL_SECURITY_PROFILE_INDEX_8));

        var aliases = getIndexResponse.getAliases().get(INTERNAL_SECURITY_PROFILE_INDEX_8);
        assertThat(aliases, hasSize(1));
        assertThat(aliases.get(0).alias(), equalTo(SECURITY_PROFILE_ALIAS));

        final Settings settings = getIndexResponse.getSettings().get(INTERNAL_SECURITY_PROFILE_INDEX_8);
        assertThat(settings.get("index.number_of_shards"), equalTo("1"));
        assertThat(settings.get("index.auto_expand_replicas"), equalTo("0-1"));
        assertThat(settings.get("index.routing.allocation.include._tier_preference"), equalTo("data_content"));

        final Map<String, Object> mappings = getIndexResponse.getMappings().get(INTERNAL_SECURITY_PROFILE_INDEX_8).getSourceAsMap();

        @SuppressWarnings("unchecked")
        final Map<String, Object> properties = (Map<String, Object>) mappings.get("properties");
        assertThat(properties, hasKey("user_profile"));

        @SuppressWarnings("unchecked")
        final Map<String, Object> userProfileProperties = (Map<String, Object>) ((Map<String, Object>) properties.get("user_profile")).get(
            "properties"
        );

        assertThat(userProfileProperties.keySet(), hasItems("uid", "enabled", "last_synchronized", "user", "access", "application_data"));
    }

    public void testGetProfileByAuthentication() {
        final ProfileService profileService = node().injector().getInstance(ProfileService.class);
        final Authentication authentication = new Authentication(
            new User("foo"),
            new Authentication.RealmRef("realm_name", "realm_type", randomAlphaOfLengthBetween(3, 8)),
            null
        );

        // Profile does not exist yet
        final PlainActionFuture<ProfileService.VersionedDocument> future1 = new PlainActionFuture<>();
        profileService.getVersionedDocument(authentication, future1);
        assertThat(future1.actionGet(), nullValue());

        // Index the document so it can be found
        final String uid2 = indexDocument();
        final PlainActionFuture<ProfileService.VersionedDocument> future2 = new PlainActionFuture<>();
        profileService.getVersionedDocument(authentication, future2);
        final ProfileService.VersionedDocument versionedDocument = future2.actionGet();
        assertThat(versionedDocument, notNullValue());
        assertThat(versionedDocument.doc().uid(), equalTo(uid2));

        // Index it again to trigger duplicate exception
        final String uid3 = indexDocument();
        final PlainActionFuture<ProfileService.VersionedDocument> future3 = new PlainActionFuture<>();
        profileService.getVersionedDocument(authentication, future3);
        final ElasticsearchException e3 = expectThrows(ElasticsearchException.class, future3::actionGet);

        assertThat(
            e3.getMessage(),
            containsString(
                "multiple [2] profiles [" + Stream.of(uid2, uid3).sorted().collect(Collectors.joining(",")) + "] found for user [foo]"
            )
        );
    }

    public void testActivateProfile() {
        final Profile profile1 = doActivateProfile(RAC_USER_NAME, TEST_PASSWORD_SECURE_STRING);
        assertThat(profile1.user().username(), equalTo(RAC_USER_NAME));
        assertThat(profile1.user().email(), nullValue());
        assertThat(profile1.user().fullName(), nullValue());

        assertThat(getProfile(profile1.uid(), Set.of()), equalTo(profile1));

        // activate again should be getting the same profile
        final Profile profile2 = doActivateProfile(RAC_USER_NAME, TEST_PASSWORD_SECURE_STRING);
        assertThat(profile2.uid(), equalTo(profile1.uid()));

        // Create another rac user in the native realm
        final PutUserRequest putUserRequest1 = new PutUserRequest();
        putUserRequest1.username(RAC_USER_NAME);
        putUserRequest1.roles("rac_role");
        final SecureString nativeRacUserPassword = new SecureString("native_rac_user_password".toCharArray());
        final String nativeRacUserPasswordHash = new String(getFastStoredHashAlgoForTests().hash(nativeRacUserPassword));
        putUserRequest1.passwordHash(nativeRacUserPasswordHash.toCharArray());
        putUserRequest1.email(RAC_USER_NAME + "@example.com");
        assertThat(client().execute(PutUserAction.INSTANCE, putUserRequest1).actionGet().created(), is(true));

        // Since file and native realms are not in the same domain yet, the new profile should be a different one
        final Profile profile3 = doActivateProfile(RAC_USER_NAME, nativeRacUserPassword);
        assertThat(profile3.uid(), not(equalTo(profile1.uid())));
        assertThat(profile3.user().email(), equalTo(RAC_USER_NAME + "@example.com"));
        assertThat(profile3.user().fullName(), nullValue());
        assertThat(profile3.access().roles(), containsInAnyOrder("rac_role"));

        // Update native rac user
        final PutUserRequest putUserRequest2 = new PutUserRequest();
        putUserRequest2.username(RAC_USER_NAME);
        putUserRequest2.roles("rac_role", "superuser");
        putUserRequest2.email(null);
        putUserRequest2.fullName("Native RAC User");
        assertThat(client().execute(PutUserAction.INSTANCE, putUserRequest2).actionGet().created(), is(false));

        // Activate again should see the updated user info
        final Profile profile4 = doActivateProfile(RAC_USER_NAME, nativeRacUserPassword);
        assertThat(profile4.uid(), equalTo(profile3.uid()));
        assertThat(profile4.user().email(), nullValue());
        assertThat(profile4.user().fullName(), equalTo("Native RAC User"));
        assertThat(profile4.access().roles(), containsInAnyOrder("rac_role", "superuser"));
    }

    private Profile doActivateProfile(String username, SecureString password) {
        final ActivateProfileRequest activateProfileRequest = new ActivateProfileRequest();
        activateProfileRequest.getGrant().setType("password");
        activateProfileRequest.getGrant().setUsername(username);
        // clone the secureString because activate action closes it afterwards
        activateProfileRequest.getGrant().setPassword(password.clone());

        final ActivateProfileResponse activateProfileResponse = client().execute(ActivateProfileAction.INSTANCE, activateProfileRequest)
            .actionGet();
        final Profile profile = activateProfileResponse.getProfile();
        assertThat(profile, notNullValue());
        assertThat(profile.user().username(), equalTo(username));
        assertThat(profile.applicationData(), anEmptyMap());
        return profile;
    }

    private Profile getProfile(String uid, Set<String> dataKeys) {
        final GetProfilesResponse getProfilesResponse = client().execute(GetProfileAction.INSTANCE, new GetProfileRequest(uid, dataKeys))
            .actionGet();
        assertThat(getProfilesResponse.getProfiles(), arrayWithSize(1));
        return getProfilesResponse.getProfiles()[0];
    }

    private String indexDocument() {
        final String uid = randomAlphaOfLength(20);
        final String source = ProfileServiceTests.SAMPLE_PROFILE_DOCUMENT_TEMPLATE.formatted(uid, Instant.now().toEpochMilli());
        client().prepareIndex(randomFrom(INTERNAL_SECURITY_PROFILE_INDEX_8, SECURITY_PROFILE_ALIAS))
            .setId("profile_" + uid)
            .setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL)
            .setSource(source, XContentType.JSON)
            .get();
        return uid;
    }
}

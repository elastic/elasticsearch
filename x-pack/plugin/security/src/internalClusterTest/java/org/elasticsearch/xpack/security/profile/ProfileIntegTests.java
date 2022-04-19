/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.profile;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.admin.indices.get.GetIndexAction;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.engine.DocumentMissingException;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.security.action.profile.GetProfileAction;
import org.elasticsearch.xpack.core.security.action.profile.GetProfileRequest;
import org.elasticsearch.xpack.core.security.action.profile.GetProfilesResponse;
import org.elasticsearch.xpack.core.security.action.profile.Profile;
import org.elasticsearch.xpack.core.security.action.profile.SetProfileEnabledAction;
import org.elasticsearch.xpack.core.security.action.profile.SetProfileEnabledRequest;
import org.elasticsearch.xpack.core.security.action.profile.SuggestProfilesAction;
import org.elasticsearch.xpack.core.security.action.profile.SuggestProfilesRequest;
import org.elasticsearch.xpack.core.security.action.profile.SuggestProfilesResponse;
import org.elasticsearch.xpack.core.security.action.profile.UpdateProfileDataAction;
import org.elasticsearch.xpack.core.security.action.profile.UpdateProfileDataRequest;
import org.elasticsearch.xpack.core.security.action.user.PutUserAction;
import org.elasticsearch.xpack.core.security.action.user.PutUserRequest;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.test.SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING;
import static org.elasticsearch.xpack.security.support.SecuritySystemIndices.INTERNAL_SECURITY_PROFILE_INDEX_8;
import static org.elasticsearch.xpack.security.support.SecuritySystemIndices.SECURITY_PROFILE_ALIAS;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItemInArray;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

public class ProfileIntegTests extends AbstractProfileIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        final Settings.Builder builder = Settings.builder().put(super.nodeSettings(nodeOrdinal, otherSettings));
        // This setting tests that the setting is registered
        builder.put("xpack.security.authc.domains.my_domain.realms", "file");
        return builder.build();
    }

    public void testProfileIndexAutoCreation() {
        // Index does not exist yet
        assertThat(getProfileIndexResponse().getIndices(), not(hasItemInArray(INTERNAL_SECURITY_PROFILE_INDEX_8)));

        // Trigger index creation by indexing
        var indexResponse = client().prepareIndex(randomFrom(INTERNAL_SECURITY_PROFILE_INDEX_8, SECURITY_PROFILE_ALIAS))
            .setSource(Map.of("user_profile", Map.of("uid", randomAlphaOfLength(22))))
            .get();
        assertThat(indexResponse.status().getStatus(), equalTo(201));

        final GetIndexResponse getIndexResponse = getProfileIndexResponse();
        assertThat(getIndexResponse.getIndices(), hasItemInArray(INTERNAL_SECURITY_PROFILE_INDEX_8));
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

        assertThat(userProfileProperties.keySet(), hasItems("uid", "enabled", "last_synchronized", "user", "labels", "application_data"));
    }

    public void testActivateProfile() {
        final Profile profile1 = doActivateProfile(RAC_USER_NAME, TEST_PASSWORD_SECURE_STRING);
        assertThat(profile1.user().username(), equalTo(RAC_USER_NAME));
        assertThat(profile1.user().roles(), contains(RAC_ROLE));
        assertThat(profile1.user().realmName(), equalTo("file"));
        assertThat(profile1.user().domainName(), equalTo("my_domain"));
        assertThat(profile1.user().email(), nullValue());
        assertThat(profile1.user().fullName(), nullValue());
        // Get by ID immediately should get the same document and content as the response to activate
        assertThat(getProfile(profile1.uid(), Set.of()), equalTo(profile1));

        // activate again should be getting the same profile
        final Profile profile2 = doActivateProfile(RAC_USER_NAME, TEST_PASSWORD_SECURE_STRING);
        assertThat(profile2.uid(), equalTo(profile1.uid()));
        // Get by ID immediately should get the same document and content as the response to activate
        assertThat(getProfile(profile2.uid(), Set.of()), equalTo(profile2));

        // Since file and native realms are not in the same domain, the new profile must be a different one
        final Profile profile3 = doActivateProfile(RAC_USER_NAME, NATIVE_RAC_USER_PASSWORD);
        assertThat(profile3.uid(), not(equalTo(profile1.uid()))); // NOT the same profile as the file user
        assertThat(profile3.user().username(), equalTo(RAC_USER_NAME));
        assertThat(profile3.user().realmName(), equalTo("index"));
        assertThat(profile3.user().domainName(), nullValue());
        assertThat(profile3.user().email(), equalTo(RAC_USER_NAME + "@example.com"));
        assertThat(profile3.user().fullName(), nullValue());
        assertThat(profile3.user().roles(), contains(RAC_ROLE));
        assertThat(profile3.labels(), anEmptyMap());
        // Get by ID immediately should get the same document and content as the response to activate
        assertThat(getProfile(profile3.uid(), Set.of()), equalTo(profile3));

        // Manually inserting some application data
        client().prepareUpdate(randomFrom(INTERNAL_SECURITY_PROFILE_INDEX_8, SECURITY_PROFILE_ALIAS), "profile_" + profile3.uid())
            .setDoc("""
                {
                    "user_profile": {
                      "labels": {
                        "my_app": {
                          "tag": "prod"
                        }
                      },
                      "application_data": {
                        "my_app": {
                          "theme": "default"
                        }
                      }
                    }
                  }
                """, XContentType.JSON)
            .setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL)
            .get();

        // Above manual update should be successful
        final Profile profile4 = getProfile(profile3.uid(), Set.of("my_app"));
        assertThat(profile4.uid(), equalTo(profile3.uid()));
        assertThat(profile4.labels(), equalTo(Map.of("my_app", Map.of("tag", "prod"))));
        assertThat(profile4.applicationData(), equalTo(Map.of("my_app", Map.of("theme", "default"))));

        // Update native rac user
        final PutUserRequest putUserRequest1 = new PutUserRequest();
        putUserRequest1.username(RAC_USER_NAME);
        putUserRequest1.roles(RAC_ROLE, "superuser");
        putUserRequest1.email(null);
        putUserRequest1.fullName("Native RAC User");
        assertThat(client().execute(PutUserAction.INSTANCE, putUserRequest1).actionGet().created(), is(false));

        // Activate again should see the updated user info
        final Profile profile5 = doActivateProfile(RAC_USER_NAME, NATIVE_RAC_USER_PASSWORD);
        assertThat(profile5.uid(), equalTo(profile3.uid()));
        assertThat(profile5.user().email(), nullValue());
        assertThat(profile5.user().fullName(), equalTo("Native RAC User"));
        assertThat(profile5.user().roles(), containsInAnyOrder(RAC_ROLE, "superuser"));
        // Re-activate should not change labels
        assertThat(profile5.labels(), equalTo(Map.of("my_app", Map.of("tag", "prod"))));
        // Get by ID immediately should get the same document and content as the response to activate
        assertThat(getProfile(profile5.uid(), Set.of()), equalTo(profile5));
        // Re-activate should not change application data
        assertThat(getProfile(profile5.uid(), Set.of("my_app")).applicationData(), equalTo(Map.of("my_app", Map.of("theme", "default"))));
    }

    public void testUpdateProfileData() {
        final Profile profile1 = doActivateProfile(RAC_USER_NAME, TEST_PASSWORD_SECURE_STRING);

        final UpdateProfileDataRequest updateProfileDataRequest1 = new UpdateProfileDataRequest(
            profile1.uid(),
            Map.of("app1", List.of("tab1", "tab2")),
            Map.of("app1", Map.of("name", "app1", "type", "app")),
            -1,
            -1,
            WriteRequest.RefreshPolicy.WAIT_UNTIL
        );
        client().execute(UpdateProfileDataAction.INSTANCE, updateProfileDataRequest1).actionGet();

        final Profile profile2 = getProfile(profile1.uid(), Set.of("app1", "app2"));

        assertThat(profile2.uid(), equalTo(profile1.uid()));
        assertThat(profile2.labels(), equalTo(Map.of("app1", List.of("tab1", "tab2"))));
        assertThat(profile2.applicationData(), equalTo(Map.of("app1", Map.of("name", "app1", "type", "app"))));

        // Update again should be incremental
        final UpdateProfileDataRequest updateProfileDataRequest2 = new UpdateProfileDataRequest(
            profile1.uid(),
            null,
            Map.of("app1", Map.of("name", "app1_take2", "active", false), "app2", Map.of("name", "app2")),
            -1,
            -1,
            WriteRequest.RefreshPolicy.WAIT_UNTIL
        );
        client().execute(UpdateProfileDataAction.INSTANCE, updateProfileDataRequest2).actionGet();

        final Profile profile3 = getProfile(profile1.uid(), Set.of("app1", "app2"));
        assertThat(profile3.uid(), equalTo(profile1.uid()));
        assertThat(profile3.labels(), equalTo(profile2.labels()));
        assertThat(
            profile3.applicationData(),
            equalTo(Map.of("app1", Map.of("name", "app1_take2", "type", "app", "active", false), "app2", Map.of("name", "app2")))
        );

        // Activate profile again should not affect the data section
        doActivateProfile(RAC_USER_NAME, TEST_PASSWORD_SECURE_STRING);
        final Profile profile4 = getProfile(profile1.uid(), Set.of("app1", "app2"));
        assertThat(profile4.labels(), equalTo(profile3.labels()));
        assertThat(profile4.applicationData(), equalTo(profile3.applicationData()));

        // Update non-existent profile should throw error
        final UpdateProfileDataRequest updateProfileDataRequest3 = new UpdateProfileDataRequest(
            "not-" + profile1.uid(),
            null,
            Map.of("foo", "bar"),
            -1,
            -1,
            WriteRequest.RefreshPolicy.WAIT_UNTIL
        );
        expectThrows(
            DocumentMissingException.class,
            () -> client().execute(UpdateProfileDataAction.INSTANCE, updateProfileDataRequest3).actionGet()
        );
    }

    public void testSuggestProfiles() {
        final String nativeRacUserPasswordHash = new String(getFastStoredHashAlgoForTests().hash(NATIVE_RAC_USER_PASSWORD));
        final Map<String, String> users = Map.of(
            "user_foo",
            "Very Curious User Foo",
            "user_bar",
            "Super Curious Admin Bar",
            "user_baz",
            "Very Anxious User Baz",
            "user_qux",
            "Super Anxious Admin Qux"
        );
        users.forEach((key, value) -> {
            final PutUserRequest putUserRequest1 = new PutUserRequest();
            putUserRequest1.username(key);
            putUserRequest1.fullName(value);
            putUserRequest1.email(key.substring(5) + "email@example.org");
            putUserRequest1.roles("rac_role");
            putUserRequest1.passwordHash(nativeRacUserPasswordHash.toCharArray());
            assertThat(client().execute(PutUserAction.INSTANCE, putUserRequest1).actionGet().created(), is(true));
            doActivateProfile(key, NATIVE_RAC_USER_PASSWORD);
        });

        final SuggestProfilesResponse.ProfileHit[] profiles1 = doSuggest("");
        assertThat(extractUsernames(profiles1), equalTo(users.keySet()));

        final SuggestProfilesResponse.ProfileHit[] profiles2 = doSuggest(randomFrom("super admin", "admin super"));
        assertThat(extractUsernames(profiles2), equalTo(Set.of("user_bar", "user_qux")));

        // Prefix match on full name
        final SuggestProfilesResponse.ProfileHit[] profiles3 = doSuggest("ver");
        assertThat(extractUsernames(profiles3), equalTo(Set.of("user_foo", "user_baz")));

        // Prefix match on the username
        final SuggestProfilesResponse.ProfileHit[] profiles4 = doSuggest("user");
        assertThat(extractUsernames(profiles4), equalTo(users.keySet()));
        // Documents scored higher are those with matches in more fields
        assertThat(extractUsernames(Arrays.copyOfRange(profiles4, 0, 2)), equalTo(Set.of("user_foo", "user_baz")));

        // Match of different terms on different fields
        final SuggestProfilesResponse.ProfileHit[] profiles5 = doSuggest(randomFrom("admin very", "very admin"));
        assertThat(extractUsernames(profiles5), equalTo(users.keySet()));

        // Match email
        final SuggestProfilesResponse.ProfileHit[] profiles6 = doSuggest(randomFrom("fooem", "fooemail"));
        assertThat(extractUsernames(profiles6), equalTo(Set.of("user_foo")));

        final SuggestProfilesResponse.ProfileHit[] profiles7 = doSuggest("example.org");
        assertThat(extractUsernames(profiles7), equalTo(users.keySet()));
    }

    public void testProfileAPIsWhenIndexNotCreated() {
        // Ensure index does not exist
        assertThat(getProfileIndexResponse().getIndices(), not(hasItemInArray(INTERNAL_SECURITY_PROFILE_INDEX_8)));

        // Get Profile by ID returns empty result
        final GetProfilesResponse getProfilesResponse = client().execute(
            GetProfileAction.INSTANCE,
            new GetProfileRequest(randomAlphaOfLength(20), Set.of())
        ).actionGet();
        assertThat(getProfilesResponse.getProfiles(), arrayWithSize(0));

        // Ensure index does not exist
        assertThat(getProfileIndexResponse().getIndices(), not(hasItemInArray(INTERNAL_SECURITY_PROFILE_INDEX_8)));

        // Search returns empty result
        final SuggestProfilesResponse.ProfileHit[] profiles1 = doSuggest("");
        assertThat(profiles1, emptyArray());

        // Ensure index does not exist
        assertThat(getProfileIndexResponse().getIndices(), not(hasItemInArray(INTERNAL_SECURITY_PROFILE_INDEX_8)));

        // Updating profile data results into doc missing exception
        // But the index is created in the process
        final DocumentMissingException e1 = expectThrows(
            DocumentMissingException.class,
            () -> client().execute(
                UpdateProfileDataAction.INSTANCE,
                new UpdateProfileDataRequest(
                    randomAlphaOfLength(20),
                    null,
                    Map.of(randomAlphaOfLengthBetween(3, 8), randomAlphaOfLengthBetween(3, 8)),
                    -1,
                    -1,
                    WriteRequest.RefreshPolicy.WAIT_UNTIL
                )
            ).actionGet()
        );

        // TODO: The index is created after the update call regardless. Should it not do that?
        assertThat(getProfileIndexResponse().getIndices(), hasItemInArray(INTERNAL_SECURITY_PROFILE_INDEX_8));
    }

    public void testSetEnabled() {
        final Profile profile1 = doActivateProfile(RAC_USER_NAME, TEST_PASSWORD_SECURE_STRING);

        final SuggestProfilesResponse.ProfileHit[] profileHits1 = doSuggest(RAC_USER_NAME);
        assertThat(profileHits1, arrayWithSize(1));
        assertThat(profileHits1[0].profile().uid(), equalTo(profile1.uid()));

        // Disable the profile
        final SetProfileEnabledRequest setProfileEnabledRequest1 = new SetProfileEnabledRequest(
            profile1.uid(),
            false,
            WriteRequest.RefreshPolicy.IMMEDIATE
        );
        client().execute(SetProfileEnabledAction.INSTANCE, setProfileEnabledRequest1).actionGet();

        // No longer visible to search
        final SuggestProfilesResponse.ProfileHit[] profileHits2 = doSuggest(RAC_USER_NAME);
        assertThat(profileHits2, emptyArray());

        // But can still direct get
        final Profile profile2 = getProfile(profile1.uid(), Set.of());
        assertThat(profile2.uid(), equalTo(profile1.uid()));
        assertThat(profile2.enabled(), is(false));

        // Enable again for search
        final SetProfileEnabledRequest setProfileEnabledRequest2 = new SetProfileEnabledRequest(
            profile1.uid(),
            true,
            WriteRequest.RefreshPolicy.IMMEDIATE
        );
        client().execute(SetProfileEnabledAction.INSTANCE, setProfileEnabledRequest2).actionGet();
        final SuggestProfilesResponse.ProfileHit[] profileHits3 = doSuggest(RAC_USER_NAME);
        assertThat(profileHits3, arrayWithSize(1));
        assertThat(profileHits3[0].profile().uid(), equalTo(profile1.uid()));

        // Enable or disable non-existing profile will throw error
        final SetProfileEnabledRequest setProfileEnabledRequest3 = new SetProfileEnabledRequest(
            "not-" + profile1.uid(),
            randomBoolean(),
            WriteRequest.RefreshPolicy.IMMEDIATE
        );
        expectThrows(
            DocumentMissingException.class,
            () -> client().execute(SetProfileEnabledAction.INSTANCE, setProfileEnabledRequest3).actionGet()
        );
    }

    private SuggestProfilesResponse.ProfileHit[] doSuggest(String query) {
        final SuggestProfilesRequest suggestProfilesRequest = new SuggestProfilesRequest(Set.of(), query, 10);
        final SuggestProfilesResponse suggestProfilesResponse = client().execute(SuggestProfilesAction.INSTANCE, suggestProfilesRequest)
            .actionGet();
        assertThat(suggestProfilesResponse.getTotalHits().relation, is(TotalHits.Relation.EQUAL_TO));
        return suggestProfilesResponse.getProfileHits();
    }

    private Set<String> extractUsernames(SuggestProfilesResponse.ProfileHit[] profileHits) {
        return Arrays.stream(profileHits)
            .map(SuggestProfilesResponse.ProfileHit::profile)
            .map(Profile::user)
            .map(Profile.ProfileUser::username)
            .collect(Collectors.toUnmodifiableSet());
    }

    private GetIndexResponse getProfileIndexResponse() {
        final GetIndexRequest getIndexRequest = new GetIndexRequest();
        getIndexRequest.indices(".*");
        return client().execute(GetIndexAction.INSTANCE, getIndexRequest).actionGet();
    }
}

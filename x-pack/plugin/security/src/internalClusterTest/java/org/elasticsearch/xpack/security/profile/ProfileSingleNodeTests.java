/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.profile;

import org.elasticsearch.action.admin.indices.get.GetIndexAction;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.engine.DocumentMissingException;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.security.action.profile.Profile;
import org.elasticsearch.xpack.core.security.action.profile.UpdateProfileDataAction;
import org.elasticsearch.xpack.core.security.action.profile.UpdateProfileDataRequest;
import org.elasticsearch.xpack.core.security.action.user.PutUserAction;
import org.elasticsearch.xpack.core.security.action.user.PutUserRequest;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.test.SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING;
import static org.elasticsearch.xpack.security.support.SecuritySystemIndices.INTERNAL_SECURITY_PROFILE_INDEX_8;
import static org.elasticsearch.xpack.security.support.SecuritySystemIndices.SECURITY_PROFILE_ALIAS;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

public class ProfileSingleNodeTests extends AbstractProfileSingleNodeTestCase {

    @Override
    protected Settings nodeSettings() {
        final Settings.Builder builder = Settings.builder().put(super.nodeSettings());
        // This setting tests that the setting is registered
        builder.put("xpack.security.authc.domains.my_domain.realms", "file");
        return builder.build();
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
        assertThat(profile3.access(), anEmptyMap());
        // Get by ID immediately should get the same document and content as the response to activate
        assertThat(getProfile(profile3.uid(), Set.of()), equalTo(profile3));

        // Manually inserting some application data
        client().prepareUpdate(randomFrom(INTERNAL_SECURITY_PROFILE_INDEX_8, SECURITY_PROFILE_ALIAS), "profile_" + profile3.uid())
            .setDoc("""
                {
                    "user_profile": {
                      "access": {
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
        assertThat(profile4.access(), equalTo(Map.of("my_app", Map.of("tag", "prod"))));
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
        // Re-activate should not change access
        assertThat(profile5.access(), equalTo(Map.of("my_app", Map.of("tag", "prod"))));
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
        assertThat(profile2.access(), equalTo(Map.of("app1", List.of("tab1", "tab2"))));
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
        assertThat(profile3.access(), equalTo(profile2.access()));
        assertThat(
            profile3.applicationData(),
            equalTo(Map.of("app1", Map.of("name", "app1_take2", "type", "app", "active", false), "app2", Map.of("name", "app2")))
        );

        // Activate profile again should not affect the data section
        doActivateProfile(RAC_USER_NAME, TEST_PASSWORD_SECURE_STRING);
        final Profile profile4 = getProfile(profile1.uid(), Set.of("app1", "app2"));
        assertThat(profile4.access(), equalTo(profile3.access()));
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
}

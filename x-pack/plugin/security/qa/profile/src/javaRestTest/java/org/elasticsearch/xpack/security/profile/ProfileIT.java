/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.profile;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.rest.ESRestTestCase;

import java.io.IOException;
import java.time.Instant;
import java.util.Map;

import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;

public class ProfileIT extends ESRestTestCase {

    public static final String SAMPLE_PROFILE_DOCUMENT_TEMPLATE = """
        {
          "uid": "%s",
          "enabled": true,
          "user": {
            "username": "foo",
            "realm": {
              "name": "realm_name",
              "type": "realm_type",
              "node_name": "node1"
            },
            "email": "foo@example.com",
            "full_name": "User Foo",
            "display_name": "Curious Foo"
          },
          "last_synchronized": %s,
          "access": {
            "roles": [
              "role1",
              "role2"
            ],
            "applications": {}
          },
          "application_data": {
            "app1": { "name": "app1" },
            "app2": { "name": "app2" }
          }
        }
        """;

    @Override
    protected Settings restAdminSettings() {
        return Settings.builder()
            .put(
                ThreadContext.PREFIX + ".Authorization",
                basicAuthHeaderValue("test_admin", new SecureString("x-pack-test-password".toCharArray()))
            )
            .build();
    }

    public void testGetProfile() throws IOException {
        final String uid = randomAlphaOfLength(20);
        final String source = SAMPLE_PROFILE_DOCUMENT_TEMPLATE.formatted(uid, Instant.now().toEpochMilli());
        final Request indexRequest = new Request("PUT", ".security-profile/_doc/profile_" + uid);
        indexRequest.setJsonEntity(source);
        indexRequest.addParameter("refresh", "wait_for");
        indexRequest.setOptions(
            expectWarnings(
                "this request accesses system indices: [.security-profile-8], but in a future major version, "
                    + "direct access to system indices will be prevented by default"
            )
        );
        assertOK(adminClient().performRequest(indexRequest));

        final Request getProfileRequest1 = new Request("GET", "_security/profile/" + uid);
        final Response getProfileResponse1 = adminClient().performRequest(getProfileRequest1);
        assertOK(getProfileResponse1);
        final Map<String, Object> getProfileMap1 = responseAsMap(getProfileResponse1);
        assertThat(getProfileMap1.keySet(), contains(uid));
        final Map<String, Object> profile1 = castToMap(getProfileMap1.get(uid));
        assertThat(castToMap(profile1.get("data")), anEmptyMap());

        // Retrieve application data along the profile
        final Request getProfileRequest2 = new Request("GET", "_security/profile/" + uid);
        getProfileRequest2.addParameter("data", "app1");
        final Map<String, Object> getProfileMap2 = responseAsMap(adminClient().performRequest(getProfileRequest2));
        assertThat(getProfileMap2.keySet(), contains(uid));
        final Map<String, Object> profile2 = castToMap(getProfileMap2.get(uid));
        assertThat(castToMap(profile2.get("data")), equalTo(Map.of("app1", Map.of("name", "app1"))));

        // Retrieve multiple application data
        final Request getProfileRequest3 = new Request("GET", "_security/profile/" + uid);
        getProfileRequest3.addParameter("data", randomFrom("app1,app2", "*", "app*"));
        final Map<String, Object> getProfileMap3 = responseAsMap(adminClient().performRequest(getProfileRequest3));
        assertThat(getProfileMap3.keySet(), contains(uid));
        final Map<String, Object> profile3 = castToMap(getProfileMap3.get(uid));
        assertThat(castToMap(profile3.get("data")), equalTo(Map.of("app1", Map.of("name", "app1"), "app2", Map.of("name", "app2"))));

        // Non-existing profile
        final Request getProfileRequest4 = new Request("GET", "_security/profile/not_" + uid);
        final ResponseException e4 = expectThrows(ResponseException.class, () -> adminClient().performRequest(getProfileRequest4));
        assertThat(e4.getResponse().getStatusLine().getStatusCode(), equalTo(404));
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> castToMap(Object o) {
        return (Map<String, Object>) o;
    }
}

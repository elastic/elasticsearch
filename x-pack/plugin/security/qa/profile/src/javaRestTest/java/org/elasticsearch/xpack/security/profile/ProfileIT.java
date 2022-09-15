/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.profile;

import org.elasticsearch.client.Node;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.test.XContentTestUtils;
import org.elasticsearch.test.rest.ESRestTestCase;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

public class ProfileIT extends ESRestTestCase {

    public static final String SAMPLE_PROFILE_DOCUMENT_TEMPLATE = """
        {
          "user_profile": {
            "uid": "%s",
            "enabled": true,
            "user": {
              "username": "%s",
              "roles": [
                "role1",
                "role2"
              ],
              "realm": {
                "name": "realm_name_1",
                "type": "realm_type_1",
                "domain": {
                  "name": "domainA",
                  "realms": [
                    { "name": "realm_name_1", "type": "realm_type_1" },
                    { "name": "realm_name_2", "type": "realm_type_2" }
                  ]
                },
                "node_name": "node1"
              },
              "email": "foo@example.com",
              "full_name": "User Foo"
            },
            "last_synchronized": %s,
            "labels": {
            },
            "application_data": {
              "app1": { "name": "app1" },
              "app2": { "name": "app2" }
            }
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

    public void testActivateProfile() throws IOException {
        final Map<String, Object> activateProfileMap = doActivateProfile();

        final String profileUid = (String) activateProfileMap.get("uid");
        final Map<String, Object> profile1 = doGetProfile(profileUid);
        assertThat(profile1, equalTo(activateProfileMap));
    }

    @SuppressWarnings("unchecked")
    public void testProfileHasPrivileges() throws IOException {
        final Map<String, Object> activateProfileMap = doActivateProfile();
        final String profileUid = (String) activateProfileMap.get("uid");
        final Request profileHasPrivilegesRequest = new Request("POST", "_security/profile/_has_privileges");
        profileHasPrivilegesRequest.setJsonEntity("""
            {
              "uids": ["some_missing_profile", "%s"],
              "privileges": {
                "index": [
                  {
                    "names": [ "rac_index_1" ],
                    "privileges": [ "read" ]
                  }
                ],
                "cluster": [
                  "cluster:monitor/health"
                ]
              }
            }""".formatted(profileUid));

        final Response profileHasPrivilegesResponse = adminClient().performRequest(profileHasPrivilegesRequest);
        assertOK(profileHasPrivilegesResponse);
        Map<String, Object> profileHasPrivilegesResponseMap = responseAsMap(profileHasPrivilegesResponse);
        assertThat(profileHasPrivilegesResponseMap.keySet(), contains("has_privilege_uids", "errors"));
        assertThat(((List<String>) profileHasPrivilegesResponseMap.get("has_privilege_uids")), contains(profileUid));
        assertThat(
            profileHasPrivilegesResponseMap.get("errors"),
            equalTo(
                Map.of(
                    "count",
                    1,
                    "details",
                    Map.of("some_missing_profile", Map.of("type", "resource_not_found_exception", "reason", "profile document not found"))
                )
            )
        );
    }

    public void testGetProfiles() throws IOException {
        final List<String> uids = randomList(1, 3, () -> randomAlphaOfLength(20));

        // Profile index does not exist yet
        final Map<String, Object> responseMap0 = doGetProfiles(uids, null);
        @SuppressWarnings("unchecked")
        final List<Object> profiles0 = (List<Object>) responseMap0.get("profiles");
        assertThat(profiles0, empty());
        final Map<String, Object> errors0 = castToMap(responseMap0.get("errors"));
        assertThat(errors0.get("count"), equalTo(uids.size()));
        final Map<String, Object> errorDetails0 = castToMap(errors0.get("details"));
        assertThat(errorDetails0.keySet(), equalTo(Set.copyOf(uids)));
        errorDetails0.values().forEach(value -> assertThat(castToMap(value).get("reason"), equalTo("profile index does not exist")));

        // Create the profile documents
        for (String uid : uids) {
            final String source = SAMPLE_PROFILE_DOCUMENT_TEMPLATE.formatted(uid, uid, Instant.now().toEpochMilli());
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
        }

        // Now retrieve profiles created above
        final Map<String, Object> responseMap1 = doGetProfiles(uids, null);
        @SuppressWarnings("unchecked")
        final List<Map<String, Object>> profiles1 = (List<Map<String, Object>>) responseMap1.get("profiles");
        assertThat(profiles1.size(), equalTo(uids.size()));
        IntStream.range(0, profiles1.size()).forEach(i -> {
            final Map<String, Object> profileMap = profiles1.get(i);
            final String uid = uids.get(i);
            assertThat(profileMap.get("uid"), equalTo(uid));
            assertThat(castToMap(profileMap.get("user")).get("username"), equalTo(uid));
            assertThat(castToMap(profileMap.get("user")).get("realm_name"), equalTo("realm_name_1"));
            assertThat(castToMap(profileMap.get("user")).get("realm_domain"), equalTo("domainA"));
            assertThat(castToMap(profileMap.get("data")), anEmptyMap());
        });

        // Retrieve application data along the profile
        final Map<String, Object> responseMap2 = doGetProfiles(uids, "app1");
        @SuppressWarnings("unchecked")
        final List<Map<String, Object>> profiles2 = (List<Map<String, Object>>) responseMap2.get("profiles");
        assertThat(profiles2.size(), equalTo(uids.size()));
        IntStream.range(0, profiles2.size()).forEach(i -> {
            final Map<String, Object> profileMap = profiles2.get(i);
            assertThat(castToMap(profileMap.get("data")), equalTo(Map.of("app1", Map.of("name", "app1"))));
        });

        // Retrieve multiple application data
        final Map<String, Object> responseMap3 = doGetProfiles(uids, randomFrom("app1,app2", "*", "app*"));
        @SuppressWarnings("unchecked")
        final List<Map<String, Object>> profiles3 = (List<Map<String, Object>>) responseMap3.get("profiles");
        assertThat(profiles3.size(), equalTo(uids.size()));
        IntStream.range(0, profiles3.size()).forEach(i -> {
            final Map<String, Object> profileMap = profiles3.get(i);
            assertThat(castToMap(profileMap.get("data")), equalTo(Map.of("app1", Map.of("name", "app1"), "app2", Map.of("name", "app2"))));
        });

        // Non-existing profiles
        final List<String> notUids = uids.stream().map(uid -> "not_" + uid).toList();
        final Map<String, Object> responseMap4 = doGetProfiles(notUids, null);
        @SuppressWarnings("unchecked")
        final List<Object> profiles4 = (List<Object>) responseMap4.get("profiles");
        assertThat(profiles4, empty());
        final Map<String, Object> errors4 = castToMap(responseMap4.get("errors"));
        assertThat(errors4.get("count"), equalTo(notUids.size()));
        final Map<String, Object> errorDetails4 = castToMap(errors4.get("details"));
        assertThat(errorDetails4.keySet(), equalTo(Set.copyOf(notUids)));
        errorDetails4.values().forEach(value -> assertThat(castToMap(value).get("type"), equalTo("resource_not_found_exception")));
    }

    public void testUpdateProfileData() throws IOException {
        final Map<String, Object> activateProfileMap = doActivateProfile();
        final String uid = (String) activateProfileMap.get("uid");
        final Request updateProfileRequest1 = new Request(randomFrom("PUT", "POST"), "_security/profile/" + uid + "/_data");
        updateProfileRequest1.setJsonEntity("""
            {
              "labels": {
                "app1": { "tags": [ "prod", "east" ] }
              },
              "data": {
                "app1": { "theme": "default" }
              }
            }""");
        assertOK(adminClient().performRequest(updateProfileRequest1));

        final Map<String, Object> profileMap1 = doGetProfile(uid, "app1");
        assertThat(castToMap(profileMap1.get("labels")), equalTo(Map.of("app1", Map.of("tags", List.of("prod", "east")))));
        assertThat(castToMap(profileMap1.get("data")), equalTo(Map.of("app1", Map.of("theme", "default"))));
    }

    public void testSuggestProfile() throws IOException {
        final Map<String, Object> activateProfileMap = doActivateProfile();
        final String uid = (String) activateProfileMap.get("uid");
        final Request suggestProfilesRequest1 = new Request(randomFrom("GET", "POST"), "_security/profile/_suggest");
        suggestProfilesRequest1.setJsonEntity("""
            {
              "name": "rac",
              "size": 10
            }""");
        final Response suggestProfilesResponse1 = adminClient().performRequest(suggestProfilesRequest1);
        assertOK(suggestProfilesResponse1);
        final Map<String, Object> suggestProfileResponseMap1 = responseAsMap(suggestProfilesResponse1);
        assertThat(suggestProfileResponseMap1, hasKey("took"));
        assertThat(suggestProfileResponseMap1.get("total"), equalTo(Map.of("value", 1, "relation", "eq")));
        @SuppressWarnings("unchecked")
        final List<Map<String, Object>> users = (List<Map<String, Object>>) suggestProfileResponseMap1.get("profiles");
        assertThat(users, hasSize(1));
        assertThat(users.get(0).get("uid"), equalTo(uid));
    }

    // Purpose of this test is to ensure the hint field works in the REST layer, e.g. parsing correctly etc.
    // It does not attempt to test whether the query works correctly once it reaches the transport and service layer
    // (other than the behaviour that hint does not decide whether a record should be returned).
    // More comprehensive tests for hint behaviours are performed in internal cluster test.
    public void testSuggestProfileWithHint() throws IOException {
        final Map<String, Object> activateProfileMap = doActivateProfile();
        final String uid = (String) activateProfileMap.get("uid");
        final Request suggestProfilesRequest1 = new Request(randomFrom("GET", "POST"), "_security/profile/_suggest");
        final String payload;
        switch (randomIntBetween(0, 2)) {
            case 0 -> {
                payload = """
                    {
                      "name": "rac",
                      "hint": {
                        "uids": ["%s"]
                      }
                    }
                    """.formatted("not-" + uid);
            }
            case 1 -> {
                payload = """
                    {
                      "name": "rac",
                      "hint": {
                        "labels": {
                          "kibana.spaces": %s
                        }
                      }
                    }
                    """.formatted(randomBoolean() ? "\"demo\"" : "[\"demo\"]");
            }
            default -> {
                payload = """
                    {
                      "name": "rac",
                      "hint": {
                        "uids": ["%s"],
                        "labels": {
                          "kibana.spaces": %s
                        }
                      }
                    }""".formatted("not-" + uid, randomBoolean() ? "\"demo\"" : "[\"demo\"]");
            }
        }
        suggestProfilesRequest1.setJsonEntity(payload);
        final Response suggestProfilesResponse1 = adminClient().performRequest(suggestProfilesRequest1);
        final Map<String, Object> suggestProfileResponseMap1 = responseAsMap(suggestProfilesResponse1);
        @SuppressWarnings("unchecked")
        final List<Map<String, Object>> users = (List<Map<String, Object>>) suggestProfileResponseMap1.get("profiles");
        assertThat(users, hasSize(1));
        assertThat(users.get(0).get("uid"), equalTo(uid));
    }

    public void testSetEnabled() throws IOException {
        final Map<String, Object> profileMap = doActivateProfile();
        final String uid = (String) profileMap.get("uid");
        doSetEnabled(uid, randomBoolean());

        // 404 for non-existing uid
        final ResponseException e1 = expectThrows(ResponseException.class, () -> doSetEnabled("not-" + uid, randomBoolean()));
        assertThat(e1.getResponse().getStatusLine().getStatusCode(), equalTo(404));
    }

    public void testSettingsOutputIncludeDomain() throws IOException {
        final Request getSettingsRequest = new Request("GET", "_cluster/settings");
        getSettingsRequest.addParameter("include_defaults", "true");
        getSettingsRequest.addParameter("filter_path", "**.security.authc.domains");
        final Response getSettingsResponse = adminClient().performRequest(getSettingsRequest);
        assertOK(getSettingsResponse);
        final XContentTestUtils.JsonMapView settingsView = XContentTestUtils.createJsonMapView(
            getSettingsResponse.getEntity().getContent()
        );

        final Map<String, Object> domainSettings1 = castToMap(settingsView.get("defaults.xpack.security.authc.domains.my_domain"));
        @SuppressWarnings("unchecked")
        final List<String> myDomainRealms = (List<String>) domainSettings1.get("realms");
        assertThat(myDomainRealms, containsInAnyOrder("default_file", "ldap1"));

        final Map<String, Object> domainSettings2 = castToMap(settingsView.get("defaults.xpack.security.authc.domains.other_domain"));
        @SuppressWarnings("unchecked")
        final List<String> otherDomainRealms = (List<String>) domainSettings2.get("realms");
        assertThat(otherDomainRealms, containsInAnyOrder("saml1", "ad1"));
    }

    public void testXpackUsageOutput() throws IOException {
        final Request xpackUsageRequest = new Request("GET", "_xpack/usage");
        xpackUsageRequest.addParameter("filter_path", "security");
        final Response xpackUsageResponse = adminClient().performRequest(xpackUsageRequest);
        assertOK(xpackUsageResponse);
        final XContentTestUtils.JsonMapView xpackUsageView = XContentTestUtils.createJsonMapView(
            xpackUsageResponse.getEntity().getContent()
        );
        final Map<String, Object> domainsUsage = castToMap(xpackUsageView.get("security.domains"));
        assertThat(domainsUsage.keySet(), equalTo(Set.of("my_domain", "other_domain")));

        @SuppressWarnings("unchecked")
        final List<String> myDomainRealms = (List<String>) castToMap(domainsUsage.get("my_domain")).get("realms");
        assertThat(myDomainRealms, containsInAnyOrder("default_file", "ldap1"));
        @SuppressWarnings("unchecked")
        final List<String> otherDomainRealms = (List<String>) castToMap(domainsUsage.get("other_domain")).get("realms");
        assertThat(otherDomainRealms, containsInAnyOrder("saml1", "ad1"));
    }

    public void testActivateGracePeriodIsPerNode() throws IOException {
        final Request activateProfileRequest = new Request("POST", "_security/profile/_activate");
        activateProfileRequest.setJsonEntity("""
            {
              "grant_type": "password",
              "username": "rac-user",
              "password": "x-pack-test-password"
            }""");

        final RestClient client = adminClient();
        final List<Node> originalNodes = client.getNodes();
        assertThat(originalNodes.size(), greaterThan(1));
        final Node node0 = originalNodes.get(0);
        // Find a different node other than node0.
        // Because all nodes of a testcluster runs on the same physical host, the different node
        // should have the same hostname but listens on a different port.
        // A single node can have both ipv4 and ipv6 addresses. If we do not filter for the
        // same hostname, we might find the same node again (e.g. node0 but has an ipv6 address).
        final Node node1 = originalNodes.subList(1, originalNodes.size() - 1)
            .stream()
            .filter(node -> node.getHost().getHostName().equals(node0.getHost().getHostName()))
            .findFirst()
            .orElseThrow();

        try {
            // Initial activate with node0
            client.setNodes(List.of(node0));
            final Map<String, Object> responseMap0 = responseAsMap(client.performRequest(activateProfileRequest));

            final Instant start = Instant.now();
            // Activate again with the same host (node0) should fall within the grace period and skip actual update
            final Map<String, Object> responseMap1 = responseAsMap(client.performRequest(activateProfileRequest));
            assumeTrue("Test is running too slow", start.plus(30, ChronoUnit.SECONDS).isAfter(Instant.now()));
            assertThat(responseMap1.get("_doc"), equalTo(responseMap0.get("_doc")));

            // Activate with different host (node1) should actually update since node name changes in RealmRef
            client.setNodes(List.of(node1));
            final Map<String, Object> responseMap2 = responseAsMap(client.performRequest(activateProfileRequest));
            assumeTrue("Test is running too slow", start.plus(30, ChronoUnit.SECONDS).isAfter(Instant.now()));
            assertThat(responseMap2.get("_doc"), not(equalTo(responseMap0.get("_doc"))));

            // Activate again with node1 should see no update
            final Map<String, Object> responseMap3 = responseAsMap(client.performRequest(activateProfileRequest));
            assertTrue("Test is running too slow", Instant.now().toEpochMilli() - (long) responseMap2.get("last_synchronized") < 30_000L);
            assertThat(responseMap3.get("_doc"), equalTo(responseMap2.get("_doc")));
        } finally {
            client.setNodes(originalNodes);
        }
    }

    public void testGetUsersWithProfileUid() throws IOException {
        final String username = randomAlphaOfLengthBetween(3, 8);
        final Request putUserRequest = new Request("PUT", "_security/user/" + username);
        putUserRequest.setJsonEntity("{\"password\":\"x-pack-test-password\",\"roles\":[\"superuser\"]}");
        assertOK(adminClient().performRequest(putUserRequest));
        final Map<String, Object> profile = doActivateProfile(username, "x-pack-test-password");

        final Request getUserRequest = new Request("GET", "_security/user" + (randomBoolean() ? "/" + username : ""));
        getUserRequest.addParameter("with_profile_uid", "true");
        final Response getUserResponse = adminClient().performRequest(getUserRequest);
        assertOK(getUserResponse);

        responseAsMap(getUserResponse).forEach((k, v) -> {
            if (username.equals(k)) {
                assertThat(castToMap(v).get("profile_uid"), equalTo(profile.get("uid")));
            } else {
                assertThat(castToMap(v), not(hasKey("profile_uid")));
            }
        });
    }

    private Map<String, Object> doActivateProfile() throws IOException {
        return doActivateProfile("rac-user", "x-pack-test-password");
    }

    private Map<String, Object> doActivateProfile(String username, String password) throws IOException {
        final Request activateProfileRequest = new Request("POST", "_security/profile/_activate");
        activateProfileRequest.setJsonEntity("""
            {
              "grant_type": "password",
              "username": "%s",
              "password": "%s"
            }""".formatted(username, password));

        final Response activateProfileResponse = adminClient().performRequest(activateProfileRequest);
        assertOK(activateProfileResponse);
        return responseAsMap(activateProfileResponse);
    }

    private Map<String, Object> doGetProfile(String uid) throws IOException {
        return doGetProfile(uid, null);
    }

    private Map<String, Object> doGetProfile(String uid, @Nullable String dataKey) throws IOException {
        final Map<String, Object> responseMap = doGetProfiles(List.of(uid), dataKey);
        assertThat(responseMap.get("errors"), nullValue());

        @SuppressWarnings("unchecked")
        final List<Map<String, Object>> profiles = (List<Map<String, Object>>) responseMap.get("profiles");
        assertThat(profiles.size(), equalTo(1));
        final Map<String, Object> profileMap = profiles.get(0);
        assertThat(profileMap.get("uid"), equalTo(uid));
        return profileMap;
    }

    private Map<String, Object> doGetProfiles(List<String> uids, @Nullable String dataKey) throws IOException {
        final Request getProfilesRequest = new Request("GET", "_security/profile/" + Strings.collectionToCommaDelimitedString(uids));
        if (dataKey != null) {
            getProfilesRequest.addParameter("data", dataKey);
        }
        final Response getProfilesResponse = adminClient().performRequest(getProfilesRequest);
        assertOK(getProfilesResponse);
        return responseAsMap(getProfilesResponse);
    }

    private void doSetEnabled(String uid, boolean enabled) throws IOException {
        final Request setEnabledRequest = new Request(
            randomFrom("PUT", "POST"),
            "_security/profile/" + uid + "/_" + (enabled ? "enable" : "disable")
        );
        adminClient().performRequest(setEnabledRequest);
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> castToMap(Object o) {
        return (Map<String, Object>) o;
    }
}

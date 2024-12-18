/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.dlsfls;

import org.apache.http.HttpHeaders;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Strings;
import org.elasticsearch.test.SecuritySettingsSourceField;
import org.elasticsearch.test.XContentTestUtils;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken;
import org.elasticsearch.xpack.security.SecurityOnTrialLicenseRestTestCase;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class FlsRestIT extends SecurityOnTrialLicenseRestTestCase {

    private static final String FLS_FILE_ROLE_USER = "fls_file_role_user";
    // defined in resources/roles.yml
    private static final String FLS_LEGACY_METADATA_FIELDS_ROLE = "fls_legacy_metadata_fields_role";
    private static final String INDEX_NAME = "index_allowed";

    @Before
    public void setup() throws IOException {
        createUser(FLS_FILE_ROLE_USER, SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING, List.of(FLS_LEGACY_METADATA_FIELDS_ROLE));
        createSystemWriteRole("system_write");
        setupData();
    }

    @After
    public void cleanup() throws IOException {
        deleteUser(FLS_FILE_ROLE_USER);
        deleteRole("system_write");
        cleanupData();
    }

    @Override
    protected Settings restAdminSettings() {
        String token = basicAuthHeaderValue("x_pack_rest_user", SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING);
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    public void testFileRoleWithUnderscoredFieldsInExceptList() throws IOException {
        searchAndAssert("x_pack_rest_user", Set.of("field1", "field2", "_field3", "hidden_field"));

        // the user has access to field* and allowlisted metadata fields like _id
        // _field3 and hidden_field are excluded due to field security
        searchAndAssert(FLS_FILE_ROLE_USER, Set.of("field1", "field2"));
    }

    public void testCannotCreateRoleWithUnderscoredFieldsInExceptList() throws IOException {
        assertThat(expectThrows(Exception.class, () -> createRole("fls_role", """
            {
              "indices": [
                {
                  "names": [
                    "index_allowed"
                  ],
                  "privileges": [
                    "read"
                  ],
                  "field_security": {
                    "grant": [
                      "field*"
                    ],
                    "except": [
                      "_field3"
                    ]
                  }
                }
              ]
            }""")).getMessage(), containsString("Exceptions for field permissions must be a subset of the granted fields"));

        assertThat(expectThrows(Exception.class, () -> createRole("fls_role", """
            {
              "indices": [
                {
                  "names": [
                    "index_allowed"
                  ],
                  "privileges": [
                    "read"
                  ],
                  "field_security": {
                    "grant": [
                      "field*"
                    ],
                    "except": [
                      "_id"
                    ]
                  }
                }
              ]
            }""")).getMessage(), containsString("Exceptions for field permissions must be a subset of the granted fields"));

        // this is allowed since "*" covers all fields in the `except` section
        assertOK(createRole("fls_role", """
            {
              "indices": [
                {
                  "names": [
                    "index_allowed"
                  ],
                  "privileges": [
                    "read"
                  ],
                  "field_security": {
                    "grant": [
                      "*"
                    ],
                    "except": [
                      "_field3",
                      "_id"
                    ]
                  }
                }
              ]
            }"""));

        // update user to have native role and check that _field3 is correctly omitted
        createUser(FLS_FILE_ROLE_USER, SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING, List.of("fls_role"));
        searchAndAssert(FLS_FILE_ROLE_USER, Set.of("hidden_field", "field1", "field2"));

        deleteRole("fls_role");
    }

    public void testExistingRolesWithUnderscoredFieldsInExceptListWork() throws IOException {
        // create valid role via API first
        assertOK(createRole("fls_role", """
            {
              "indices": [
                {
                  "names": [
                    "index_allowed"
                  ],
                  "privileges": [
                    "read"
                  ],
                  "field_security": {
                    "grant": [
                      "field*"
                    ],
                    "except": [
                      "field1"
                    ]
                  }
                }
              ]
            }"""));

        // update user to have native role and check that _field3 is correctly omitted
        createUser(FLS_FILE_ROLE_USER, SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING, List.of("fls_role"));
        searchAndAssert(FLS_FILE_ROLE_USER, Set.of("field2"));

        String systemWriteCreds = createUser("superuser_with_system_write", new String[] { "superuser", "system_write" });
        // via security index access, update the role to exclude _field3, bypassing validation
        updateRoles(systemWriteCreds, "ctx._source['indices'][0]['field_security']['except']=['_field3'];", List.of("fls_role"));

        searchAndAssert(FLS_FILE_ROLE_USER, Set.of("field1", "field2"));

        deleteUser("superuser_with_system_write");
    }

    static void createSystemWriteRole(String roleName) throws IOException {
        assertOK(createRole(roleName, """
            {
              "cluster": ["all"],
              "indices": [
                {
                  "names": [ "*" ],
                  "privileges": ["all"],
                  "allow_restricted_indices" : true
                }
              ]
            }"""));
    }

    static String createUser(String username, String[] roles) throws IOException {
        final Request request = new Request("POST", "/_security/user/" + username);
        Map<String, Object> body = Map.ofEntries(Map.entry("roles", roles), Map.entry("password", "super-strong-password".toString()));
        request.setJsonEntity(XContentTestUtils.convertToXContent(body, XContentType.JSON).utf8ToString());
        Response response = adminClient().performRequest(request);
        assertOK(response);
        return basicAuthHeaderValue(username, new SecureString("super-strong-password".toCharArray()));
    }

    private static Response createRole(String role, String payload) throws IOException {
        final Request createRoleRequest = new Request("POST", "/_security/role/" + role);
        createRoleRequest.setJsonEntity(payload);
        return adminClient().performRequest(createRoleRequest);
    }

    private void searchAndAssert(String user, Set<String> expectedSourceFields) throws IOException {
        final Request searchRequest = new Request("GET", INDEX_NAME + "/_search");
        searchRequest.setOptions(
            RequestOptions.DEFAULT.toBuilder()
                .addHeader(
                    "Authorization",
                    UsernamePasswordToken.basicAuthHeaderValue(user, SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING)
                )
        );
        assertSearchResponse(client().performRequest(searchRequest), "1", expectedSourceFields);
    }

    @SuppressWarnings("unchecked")
    private void assertSearchResponse(Response response, String docId, Set<String> sourceFields) throws IOException {
        final Map<String, Object> m = responseAsMap(response);

        final Map<String, Object> hits = (Map<String, Object>) m.get("hits");
        final List<Map<String, Object>> docs = (List<Map<String, Object>>) hits.get("hits");

        assertThat(docs.size(), equalTo(1));
        assertThat(docs.getFirst().get("_id"), equalTo(docId));

        var actualFields = ((Map<String, Object>) docs.getFirst().get("_source")).keySet();
        assertThat(actualFields, equalTo(sourceFields));
    }

    private void setupData() throws IOException {
        final Request createRequest = new Request("PUT", INDEX_NAME);
        createRequest.setJsonEntity("""
            {}""");
        final Response createResponse = adminClient().performRequest(createRequest);
        assertOK(createResponse);
        ensureGreen(INDEX_NAME);

        final Request indexRequest = new Request("POST", INDEX_NAME + "/_doc/1?refresh=true");
        indexRequest.setJsonEntity("""
            {"hidden_field": "0", "field1": "1", "field2": "2", "_field3": "3"}""");
        final Response indexResponse = adminClient().performRequest(indexRequest);
        assertOK(indexResponse);
    }

    private void cleanupData() throws IOException {
        final Request deleteRequest = new Request("DELETE", INDEX_NAME);
        final Response deleteResponse = adminClient().performRequest(deleteRequest);
        assertOK(deleteResponse);
    }

    static void expectWarnings(Request request, String... expectedWarnings) {
        final Set<String> expected = Set.of(expectedWarnings);
        RequestOptions options = request.getOptions().toBuilder().setWarningsHandler(warnings -> {
            final Set<String> actual = Set.copyOf(warnings);
            // Return true if the warnings aren't what we expected; the client will treat them as a fatal error.
            return actual.equals(expected) == false;
        }).build();
        request.setOptions(options);
    }

    static void updateRoles(String creds, String script, Collection<String> roleNames) throws IOException {
        if (roleNames.isEmpty()) {
            return;
        }
        final Request request = new Request("POST", "/.security/_update_by_query?refresh=true&wait_for_completion=true");
        request.setJsonEntity(Strings.format("""
            {
              "script": {
                "source": "%s",
                "lang": "painless"
              },
              "query": {
                "bool": {
                  "must": [
                    {"term": {"type": "role"}},
                    {"ids": {"values": %s}}
                  ]
                }
              }
            }
            """, script, roleNames.stream().map(id -> "\"role-" + id + "\"").collect(Collectors.toList())));
        request.setOptions(request.getOptions().toBuilder().addHeader(HttpHeaders.AUTHORIZATION, creds));
        expectWarnings(
            request,
            "this request accesses system indices: [.security-7],"
                + " but in a future major version, direct access to system indices will be prevented by default"
        );
        Response response = client().performRequest(request);
        assertOK(response);
    }
}

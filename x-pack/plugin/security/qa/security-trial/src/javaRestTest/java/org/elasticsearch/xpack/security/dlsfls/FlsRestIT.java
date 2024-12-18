/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.dlsfls;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.SecuritySettingsSourceField;
import org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken;
import org.elasticsearch.xpack.security.SecurityOnTrialLicenseRestTestCase;
import org.elasticsearch.xpack.security.authc.ApiKeyService;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

    public void testCreateRoleWithUnderscoredFieldsInExceptList() throws IOException {
        searchAndAssert("x_pack_rest_user", Set.of("field1", "field2", "_field3", "hidden_field"));

        // update user to have native role and check that _field3 is correctly omitted
        createUser(FLS_FILE_ROLE_USER, SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING, List.of("fls_role"));

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
                      "_id"
                    ]
                  }
                }
              ]
            }
            """));
        searchAndAssert(FLS_FILE_ROLE_USER, Set.of("hidden_field", "field1", "field2", "_field3"));

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
                      "_id"
                    ]
                  }
                }
              ]
            }
            """));
        searchAndAssert(FLS_FILE_ROLE_USER, Set.of("field1", "field2"));

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
                    ]
                  }
                }
              ]
            }
            """));
        searchAndAssert(FLS_FILE_ROLE_USER, Set.of("field1", "field2"));

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
                      "_field3"
                    ]
                  }
                }
              ]
            }
            """));
        searchAndAssert(FLS_FILE_ROLE_USER, Set.of("field1", "field2"));

        // this is not valid because even with the legacy metadata fields automaton, the except list is not a proper subset
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
                      "*_field3"
                    ]
                  }
                }
              ]
            }
            """)).getMessage(), containsString("Exceptions for field permissions must be a subset of the granted fields"));

        deleteRole("fls_role");
    }

    public void testExistingRolesWithUnderscoredFieldsInExceptListWork() throws Exception {
        searchAndAssert(FLS_FILE_ROLE_USER, Set.of("field1", "field2"));

        String k = createApiKey(FLS_FILE_ROLE_USER, """
            {
              "name": "fls_api_key",
              "role_descriptors": {
                "fls_role": {
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
                          "field2",
                          "_field3",
                          "_id"
                        ]
                      }
                    }
                  ]
                }
              }
            }""");

        searchAndAssertWithAuthzHeader(k, Set.of("field1"));
    }

    public void testCannotCreateApiKeyWithUnderscoredFieldsInExceptList() throws IOException {
        String k = createApiKey("x_pack_rest_user", """
            {
              "name": "fls_api_key",
              "role_descriptors": {
                "fls_role": {
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
                          "_field4"
                        ]
                      }
                    }
                  ]
                }
              }
            }""");
        searchAndAssertWithAuthzHeader(k, Set.of("field1", "field2"));

        k = createApiKey("x_pack_rest_user", """
            {
              "name": "fls_api_key",
              "role_descriptors": {
                "fls_role": {
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
                          "_id",
                          "_field3"
                        ]
                      }
                    }
                  ]
                }
              }
            }""");
        searchAndAssertWithAuthzHeader(k, Set.of("field1", "field2"));

        k = createApiKey("x_pack_rest_user", """
            {
              "name": "fls_api_key",
              "role_descriptors": {
                "fls_role": {
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
                          "*field*"
                        ],
                        "except": [
                          "_field3"
                        ]
                      }
                    }
                  ]
                }
              }
            }""");
        searchAndAssertWithAuthzHeader(k, Set.of("hidden_field", "field1", "field2"));

        k = createApiKey("x_pack_rest_user", """
            {
              "name": "fls_api_key",
              "role_descriptors": {
                "fls_role": {
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
                }
              }
            }""");

        searchAndAssertWithAuthzHeader(k, Set.of("hidden_field", "field1", "field2"));
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

    private static Response createRole(String role, String payload) throws IOException {
        final Request createRoleRequest = new Request("POST", "/_security/role/" + role);
        createRoleRequest.setJsonEntity(payload);
        return adminClient().performRequest(createRoleRequest);
    }

    private static String createApiKey(String user, String payload) throws IOException {
        final Request createApiKeyRequest = new Request("POST", "/_security/api_key");
        createApiKeyRequest.setJsonEntity(payload);
        createApiKeyRequest.setOptions(
            RequestOptions.DEFAULT.toBuilder()
                .addHeader(
                    "Authorization",
                    UsernamePasswordToken.basicAuthHeaderValue(user, SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING)
                )
        );
        var response = client().performRequest(createApiKeyRequest);
        assertOK(response);
        return ApiKeyService.withApiKeyPrefix(responseAsMap(response).get("encoded").toString());
    }

    private void searchAndAssert(String user, Set<String> expectedSourceFields) throws IOException {
        searchAndAssertWithAuthzHeader(
            basicAuthHeaderValue(user, SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING),
            expectedSourceFields
        );
    }

    private void searchAndAssertWithAuthzHeader(String authzHeader, Set<String> expectedSourceFields) throws IOException {
        final Request searchRequest = new Request("GET", INDEX_NAME + "/_search");
        searchRequest.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", authzHeader));
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
}

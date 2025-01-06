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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class FlsMetadataFieldsRestIT extends SecurityOnTrialLicenseRestTestCase {
    private static final String FLS_FILE_ROLE_USER = "fls_file_role_user";
    // defined in resources/roles.yml
    private static final String FLS_LEGACY_METADATA_FIELDS_ROLE = "fls_legacy_metadata_fields_role";
    private static final String INDEX_NAME = "index_allowed";
    public static final Set<String> EXPECTED_METADATA_FIELDS = Set.of(
        "_routing",
        "_doc_count",
        "_ignored_source",
        "_index",
        "_feature",
        "_index_mode",
        "_ignored",
        "_tier",
        "_seq_no",
        "_nested_path",
        "_data_stream_timestamp",
        "_field_names",
        "_source",
        "_id",
        "_version"
    );

    @Before
    public void setup() throws IOException {
        createUser(FLS_FILE_ROLE_USER, SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING, List.of(FLS_LEGACY_METADATA_FIELDS_ROLE));
        setupData();
    }

    @After
    public void cleanup() throws IOException {
        deleteUser(FLS_FILE_ROLE_USER);
        cleanupData();
    }

    @Override
    protected Settings restAdminSettings() {
        String token = basicAuthHeaderValue("x_pack_rest_user", SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING);
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    public void testFileRoleWithUnderscoredFieldsInExceptList() throws IOException {
        queryAndAssert("x_pack_rest_user", Set.of("field1", "field2", "_field3", "hidden_field"));

        // the user has access to field* and allowlisted metadata fields like _id
        // _field3 and hidden_field are excluded due to field security
        queryAndAssert(FLS_FILE_ROLE_USER, Set.of("field1", "field2"));
    }

    public void testCreateRoleWithUnderscoredFieldsInExceptList() throws IOException {
        queryAndAssert("x_pack_rest_user", Set.of("field1", "field2", "_field3", "hidden_field"));

        createUser(FLS_FILE_ROLE_USER, SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING, List.of("fls_role"));

        assertOK(createRole("fls_role", """
            {
              "indices": [
                {
                  "names": [
                    "index_allowed"
                  ],
                  "privileges": [
                    "read",
                    "view_index_metadata"
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
        queryAndAssert(FLS_FILE_ROLE_USER, Set.of("hidden_field", "field1", "field2", "_field3"));

        assertOK(createRole("fls_role", """
            {
              "indices": [
                {
                  "names": [
                    "index_allowed"
                  ],
                  "privileges": [
                    "read",
                    "view_index_metadata"
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
        queryAndAssert(FLS_FILE_ROLE_USER, Set.of("field1", "field2"));

        assertOK(createRole("fls_role", """
            {
              "indices": [
                {
                  "names": [
                    "index_allowed"
                  ],
                  "privileges": [
                    "read",
                    "view_index_metadata"
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
        queryAndAssert(FLS_FILE_ROLE_USER, Set.of("field1", "field2"));

        assertOK(createRole("fls_role", """
            {
              "indices": [
                {
                  "names": [
                    "index_allowed"
                  ],
                  "privileges": [
                    "read",
                    "view_index_metadata"
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
        queryAndAssert(FLS_FILE_ROLE_USER, Set.of("field1", "field2"));

        // this is not valid because even with the legacy metadata fields automaton, the except list is not a proper subset
        assertThat(expectThrows(Exception.class, () -> createRole("fls_role", """
            {
              "indices": [
                {
                  "names": [
                    "index_allowed"
                  ],
                  "privileges": [
                    "read",
                    "view_index_metadata"
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

    public void testCreateApiKeyWithUnderscoredFieldsInExceptListWork() throws Exception {
        queryAndAssert(FLS_FILE_ROLE_USER, Set.of("field1", "field2"));

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
                        "read",
                        "view_index_metadata"
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
                        "read",
                        "view_index_metadata"
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
                        "read",
                        "view_index_metadata"
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
                        "read",
                        "view_index_metadata"
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
                        "read",
                        "view_index_metadata"
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

    // TODO this should be query and assert with authz header
    private void queryAndAssert(String user, Set<String> expectedFields) throws IOException {
        searchAndAssertWithAuthzHeader(basicAuthHeaderValue(user, SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING), expectedFields);

        getAndAssertWithAuthzHeader(basicAuthHeaderValue(user, SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING), expectedFields);

        var expectedFieldsWithMetadataFields = new HashSet<>(EXPECTED_METADATA_FIELDS);
        expectedFieldsWithMetadataFields.addAll(expectedFields);

        getFieldMappingsAndAssertWithAuthzHeader(
            basicAuthHeaderValue(user, SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING),
            expectedFieldsWithMetadataFields
        );

        getFieldCapsAndAssertWithAuthzHeader(
            basicAuthHeaderValue(user, SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING),
            expectedFieldsWithMetadataFields
        );

        // search with `fields`?
    }

    private void searchAndAssertWithAuthzHeader(String authzHeader, Set<String> expectedFields) throws IOException {
        final Request request = new Request("GET", INDEX_NAME + "/_search");
        request.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", authzHeader));
        assertSearchResponse(client().performRequest(request), "1", expectedFields);
    }

    private void getAndAssertWithAuthzHeader(String authzHeader, Set<String> expectedFields) throws IOException {
        final Request request = new Request("GET", INDEX_NAME + "/_doc/1");
        request.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", authzHeader));
        assertGetResponse(client().performRequest(request), "1", expectedFields);
    }

    private void getFieldMappingsAndAssertWithAuthzHeader(String authzHeader, Set<String> expectedFields) throws IOException {
        final Request request = new Request("GET", INDEX_NAME + "/_mapping/field/*");
        request.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", authzHeader));
        assertGetFieldMappings(client().performRequest(request), expectedFields);
    }

    private void getFieldCapsAndAssertWithAuthzHeader(String authzHeader, Set<String> expectedFields) throws IOException {
        final Request request = new Request("GET", INDEX_NAME + "/_field_caps?fields=*");
        request.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", authzHeader));
        assertGetFieldCaps(client().performRequest(request), expectedFields);
    }

    @SuppressWarnings("unchecked")
    private void assertGetFieldCaps(Response response, Set<String> expectedFields) throws IOException {
        final Map<String, Object> m = responseAsMap(response);
        final Map<String, Object> fields = (Map<String, Object>) m.get("fields");
        assertThat(fields.keySet(), equalTo(expectedFields));
    }

    @SuppressWarnings("unchecked")
    private void assertSearchResponse(Response response, String docId, Set<String> expectedFields) throws IOException {
        final Map<String, Object> m = responseAsMap(response);

        final Map<String, Object> hits = (Map<String, Object>) m.get("hits");
        final List<Map<String, Object>> docs = (List<Map<String, Object>>) hits.get("hits");

        assertThat(docs.size(), equalTo(1));
        assertThat(docs.getFirst().get("_id"), equalTo(docId));

        var actualFields = ((Map<String, Object>) docs.getFirst().get("_source")).keySet();
        assertThat(actualFields, equalTo(expectedFields));
    }

    @SuppressWarnings("unchecked")
    private void assertGetResponse(Response response, String docId, Set<String> expectedFields) throws IOException {
        final Map<String, Object> m = responseAsMap(response);
        assertThat(m.get("_id"), equalTo(docId));

        var actualFields = ((Map<String, Object>) m.get("_source")).keySet();
        assertThat(actualFields, equalTo(expectedFields));
    }

    @SuppressWarnings("unchecked")
    private void assertGetFieldMappings(Response response, Set<String> expectedFields) throws IOException {
        final Map<String, Object> m = responseAsMap(response);
        final Map<String, Object> mappings = (Map<String, Object>) m.get(INDEX_NAME);
        final Map<String, Object> fieldMappings = (Map<String, Object>) mappings.get("mappings");

        var actualFields = fieldMappings.keySet();

        assertThat(actualFields, equalTo(expectedFields));
    }

    private void setupData() throws IOException {
        final Request createRequest = new Request("PUT", INDEX_NAME);
        createRequest.setJsonEntity("""
            {
              "mappings": {
                "properties": {
                  "hidden_field": { "type": "keyword" },
                  "field1": { "type": "keyword" },
                  "field2": { "type": "keyword" },
                  "_field3": { "type": "keyword" }
                }
              }
            }""");
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

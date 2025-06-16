/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.failurestore;

import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.elasticsearch.Build;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchResponseUtils;
import org.elasticsearch.test.TestSecurityClient;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.ObjectPath;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.SecurityOnTrialLicenseRestTestCase;
import org.hamcrest.Matcher;
import org.junit.Before;
import org.junit.ClassRule;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.iterableWithSize;
import static org.hamcrest.Matchers.notNullValue;

public class FailureStoreSecurityRestIT extends ESRestTestCase {

    private TestSecurityClient securityClient;

    private Map<String, String> apiKeys = new HashMap<>();

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .apply(SecurityOnTrialLicenseRestTestCase.commonTrialSecurityClusterConfig)
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected Settings restAdminSettings() {
        String token = basicAuthHeaderValue("admin_user", new SecureString("admin-password".toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    private static final String ASYNC_SEARCH_TIMEOUT = "30s";

    private static final String ADMIN_USER = "admin_user";

    private static final String DATA_ACCESS = "data_access";
    private static final String BACKING_INDEX_DATA_ACCESS = "backing_index_data_access";
    private static final String BACKING_INDEX_FAILURE_ACCESS = "backing_index_failure_access";
    private static final String FAILURE_INDEX_DATA_ACCESS = "failure_index_data_access";
    private static final String FAILURE_INDEX_FAILURE_ACCESS = "failure_index_failure_access";
    private static final String STAR_READ_ONLY_ACCESS = "star_read_only";
    private static final String FAILURE_STORE_ACCESS = "failure_store_access";
    private static final String BOTH_ACCESS = "both_access";
    private static final String WRITE_ACCESS = "write_access";
    private static final String MANAGE_ACCESS = "manage_access";
    private static final String MANAGE_FAILURE_STORE_ACCESS = "manage_failure_store_access";
    private static final String MANAGE_DATA_STREAM_LIFECYCLE = "manage_data_stream_lifecycle";
    private static final SecureString PASSWORD = new SecureString("admin-password");

    @Before
    public void setup() throws IOException {
        apiKeys = new HashMap<>();
        createUser(WRITE_ACCESS, PASSWORD, WRITE_ACCESS);
        upsertRole(Strings.format("""
            {
              "cluster": ["all"],
              "indices": [{"names": ["test*", "other*"], "privileges": ["write", "auto_configure"]}]
            }"""), WRITE_ACCESS);
    }

    public void testGetUserPrivileges() throws IOException {
        createUser("user", PASSWORD, "role");

        upsertRole("""
            {
              "cluster": ["all"],
              "indices": [
                {
                  "names": ["*"],
                  "privileges": ["read", "read_failure_store"]
                }
              ]
            }
            """, "role");
        expectUserPrivilegesResponse("""
            {
              "cluster": ["all"],
              "global": [],
              "indices": [{
                  "names": ["*"],
                  "privileges": ["read", "read_failure_store"],
                  "allow_restricted_indices": false
                }
              ],
              "applications": [],
              "run_as": []
            }""");

        upsertRole("""
            {
              "cluster": ["all"],
              "indices": [
                {
                  "names": ["*"],
                  "privileges": ["read_failure_store"]
                }
              ]
            }
            """, "role");
        expectUserPrivilegesResponse("""
            {
              "cluster": ["all"],
              "global": [],
              "indices": [
                {
                  "names": ["*"],
                  "privileges": ["read_failure_store"],
                  "allow_restricted_indices": false
                }],
              "applications": [],
              "run_as": []
            }""");

        upsertRole("""
            {
              "cluster": ["all"],
              "indices": [
                {
                  "names": ["*"],
                  "privileges": ["all", "read_failure_store"]
                }
              ]
            }
            """, "role");
        expectUserPrivilegesResponse("""
            {
              "cluster": ["all"],
              "global": [],
              "indices": [
                {
                  "names": ["*"],
                  "privileges": ["all", "read_failure_store"],
                  "allow_restricted_indices": false
                }],
              "applications": [],
              "run_as": []
            }""");

        upsertRole("""
            {
              "cluster": ["all"],
              "indices": [
                {
                  "names": ["*"],
                  "privileges": ["read", "read_failure_store"]
                },
                {
                  "names": ["*"],
                  "privileges": ["write", "manage_failure_store"]
                }
              ]
            }
            """, "role");
        expectUserPrivilegesResponse("""
            {
              "cluster": ["all"],
              "global": [],
              "indices": [
                {
                  "names": ["*"],
                  "privileges": ["manage_failure_store", "read", "read_failure_store", "write"],
                  "allow_restricted_indices": false
                }
              ],
              "applications": [],
              "run_as": []
            }""");

        upsertRole("""
            {
              "cluster": ["all"],
              "indices": [
                {
                  "names": ["*", "idx"],
                  "privileges": ["read", "manage"],
                  "allow_restricted_indices": false
                },
                {
                  "names": ["idx", "*"],
                  "privileges": ["manage_data_stream_lifecycle"],
                  "allow_restricted_indices": false
                },
                {
                  "names": ["*", "idx"],
                  "privileges": ["write"],
                  "allow_restricted_indices": true
                },
                {
                  "names": ["idx", "*"],
                  "privileges": ["manage"],
                  "allow_restricted_indices": true
                }
              ]
            }
            """, "role");
        expectUserPrivilegesResponse("""
            {
              "cluster": ["all"],
              "global": [],
              "indices": [
                {
                  "names": ["*", "idx"],
                  "privileges": ["manage", "manage_data_stream_lifecycle", "read"],
                  "allow_restricted_indices": false
                },
                {
                  "names": ["*", "idx"],
                  "privileges": ["manage", "write"],
                  "allow_restricted_indices": true
                }
              ],
              "applications": [],
              "run_as": []
            }""");
    }

    public void testHasPrivileges() throws IOException {
        createUser("user", PASSWORD, "role");

        upsertRole("""
            {
              "cluster": ["all"],
              "indices": [
                {
                  "names": ["*"],
                  "privileges": ["read", "read_failure_store"]
                },
                {
                  "names": ["test2"],
                  "privileges": ["manage_failure_store", "write"]
                }
              ]
            }
            """, "role");
        createAndStoreApiKey("user", randomBoolean() ? null : """
            {
              "role": {
                "cluster": ["all"],
                "indices": [
                  {
                    "names": ["*"],
                    "privileges": ["read", "read_failure_store"]
                  },
                  {
                    "names": ["test2"],
                    "privileges": ["manage_failure_store", "write"]
                  }
                ]
              }
            }
            """);

        expectHasPrivileges("user", """
            {
                "index": [
                    {
                        "names": ["test1"],
                        "privileges": ["read", "read_failure_store"]
                    },
                    {
                        "names": ["test2"],
                        "privileges": ["read"]
                    },
                    {
                        "names": ["test2"],
                        "privileges": ["read_failure_store"]
                    },
                    {
                        "names": ["test1"],
                        "privileges": ["manage_failure_store"]
                    },
                    {
                        "names": ["test1"],
                        "privileges": ["manage"]
                    },
                    {
                        "names": ["test2"],
                        "privileges": ["manage_failure_store"]
                    },
                    {
                        "names": ["test2"],
                        "privileges": ["manage"]
                    }
                ]
            }
            """, """
            {
                "username": "user",
                "has_all_requested": false,
                "cluster": {},
                "index": {
                    "test1": {
                        "read": true,
                        "read_failure_store": true,
                        "manage_failure_store": false,
                        "manage": false
                    },
                    "test2": {
                        "read": true,
                        "read_failure_store": true,
                        "manage_failure_store": true,
                        "manage": false
                    }
                },
                "application": {}
            }
            """);
        expectHasPrivileges("user", """
            {
                "index": [
                    {
                        "names": ["test1"],
                        "privileges": ["indices:data/write/*"]
                    },
                    {
                        "names": ["test2"],
                        "privileges": ["indices:admin/*", "indices:data/write/*"]
                    }
                ]
            }
            """, """
            {
                "username": "user",
                "has_all_requested": false,
                "cluster": {},
                "index": {
                    "test1": {
                        "indices:data/write/*": false
                    },
                    "test2": {
                        "indices:admin/*": false,
                        "indices:data/write/*": true
                    }
                },
                "application": {}
            }
            """);
        expectHasPrivileges("user", """
            {
                "index": [
                    {
                        "names": ["test1"],
                        "privileges": ["indices:data/write/*"]
                    }
                ]
            }
            """, """
            {
                "username": "user",
                "has_all_requested": false,
                "cluster": {},
                "index": {
                    "test1": {
                        "indices:data/write/*": false
                    }
                },
                "application": {}
            }
            """);
        expectHasPrivileges("user", """
            {
                "index": [
                    {
                        "names": ["test1"],
                        "privileges": ["read"]
                    }
                ]
            }
            """, """
            {
                "username": "user",
                "has_all_requested": true,
                "cluster": {},
                "index": {
                    "test1": {
                        "read": true
                    }
                },
                "application": {}
            }
            """);
        expectHasPrivileges("user", """
            {
                "index": [
                    {
                        "names": ["test1"],
                        "privileges": ["read_failure_store"]
                    }
                ]
            }
            """, """
            {
                "username": "user",
                "has_all_requested": true,
                "cluster": {},
                "index": {
                    "test1": {
                        "read_failure_store": true
                    }
                },
                "application": {}
            }
            """);
        expectHasPrivileges("user", """
            {
                "index": [
                    {
                        "names": [".security-7"],
                        "privileges": ["read_failure_store"],
                        "allow_restricted_indices": true
                    }
                ]
            }
            """, """
            {
                "username": "user",
                "has_all_requested": false,
                "cluster": {},
                "index": {
                    ".security-7": {
                        "read_failure_store": false
                    }
                },
                "application": {}
            }
            """);
        expectHasPrivileges("user", """
            {
                "index": [
                    {
                        "names": [".security-7", "test1"],
                        "privileges": ["read_failure_store"],
                        "allow_restricted_indices": true
                    }
                ]
            }
            """, """
            {
                "username": "user",
                "has_all_requested": false,
                "cluster": {},
                "index": {
                    ".security-7": {
                        "read_failure_store": false
                    },
                    "test1": {
                        "read_failure_store": true
                    }
                },
                "application": {}
            }
            """);

        upsertRole("""
            {
              "cluster": ["all"],
              "indices": [
                {
                  "names": ["*"],
                  "privileges": ["indices:data/read/*"]
                },
                {
                  "names": ["test*"],
                  "privileges": ["read_failure_store"]
                },
                {
                  "names": ["test2"],
                  "privileges": ["all"]
                }
              ]
            }
            """, "role");
        apiKeys.remove("user");
        createAndStoreApiKey("user", randomBoolean() ? null : """
            {
                "role": {
                  "cluster": ["all"],
                  "indices": [
                    {
                      "names": ["*"],
                      "privileges": ["indices:data/read/*"]
                    },
                    {
                      "names": ["test*"],
                      "privileges": ["read_failure_store"]
                    },
                    {
                      "names": ["test2"],
                      "privileges": ["all"]
                    }
                  ]
                }
            }
            """);
        expectHasPrivileges("user", """
            {
                "index": [
                    {
                        "names": ["test1"],
                        "privileges": ["all", "indices:data/read/*", "read", "read_failure_store", "write"]
                    },
                    {
                        "names": ["test2"],
                        "privileges": ["all", "indices:data/read/*", "read", "read_failure_store", "write"]
                    },
                    {
                        "names": ["test3"],
                        "privileges": ["all", "indices:data/read/*", "read", "read_failure_store", "write"]
                    }
                ]
            }
            """, """
            {
                "username": "user",
                "has_all_requested": false,
                "cluster": {},
                "index": {
                    "test1": {
                        "all": false,
                        "indices:data/read/*": true,
                        "read": false,
                        "read_failure_store": true,
                        "write": false
                    },
                    "test2": {
                        "all": true,
                        "indices:data/read/*": true,
                        "read": true,
                        "read_failure_store": true,
                        "write": true
                    },
                    "test3": {
                        "all": false,
                        "indices:data/read/*": true,
                        "read": false,
                        "read_failure_store": true,
                        "write": false
                    }
                },
                "application": {}
            }
            """);

        upsertRole("""
            {
              "cluster": ["all"],
              "indices": [
                {
                  "names": ["test1"],
                  "privileges": ["read", "read_failure_store"]
                }
              ]
            }
            """, "role");
        apiKeys.remove("user");
        createAndStoreApiKey("user", randomBoolean() ? null : """
            {
                "role": {
                  "cluster": ["all"],
                  "indices": [
                    {
                      "names": ["test1"],
                      "privileges": ["read", "read_failure_store"]
                    }
                  ]
                }
            }
            """);
        expectHasPrivileges("user", """
            {
                "index": [
                    {
                        "names": ["test1"],
                        "privileges": ["all"]
                    }
                ]
            }
            """, """
            {
                "username": "user",
                "has_all_requested": false,
                "cluster": {},
                "index": {
                    "test1": {
                        "all": false
                    }
                },
                "application": {}
            }
            """);

        upsertRole("""
            {
              "cluster": ["all"],
              "indices": [
                {
                  "names": ["test1"],
                  "privileges": ["all"]
                }
              ]
            }
            """, "role");
        apiKeys.remove("user");
        createAndStoreApiKey("user", randomBoolean() ? null : """
            {
                "role": {
                  "cluster": ["all"],
                  "indices": [
                    {
                      "names": ["test1"],
                      "privileges": ["all"]
                    }
                  ]
                }
            }
            """);
        expectHasPrivileges("user", """
            {
                "index": [
                    {
                        "names": ["test1"],
                        "privileges": ["all"]
                    }
                ]
            }
            """, """
            {
                "username": "user",
                "has_all_requested": true,
                "cluster": {},
                "index": {
                    "test1": {
                        "all": true
                    }
                },
                "application": {}
            }
            """);
        expectHasPrivileges("user", """
            {
                "index": [
                    {
                        "names": ["test1"],
                        "privileges": ["read"]
                    }
                ]
            }
            """, """
            {
                "username": "user",
                "has_all_requested": true,
                "cluster": {},
                "index": {
                    "test1": {
                        "read": true
                    }
                },
                "application": {}
            }
            """);
        expectHasPrivileges("user", """
            {
                "index": [
                    {
                        "names": ["test1"],
                        "privileges": ["read_failure_store"]
                    }
                ]
            }
            """, """
            {
                "username": "user",
                "has_all_requested": true,
                "cluster": {},
                "index": {
                    "test1": {
                        "read_failure_store": true
                    }
                },
                "application": {}
            }
            """);
        expectHasPrivileges("user", """
            {
                "index": [
                    {
                        "names": [".security-7"],
                        "privileges": ["read_failure_store", "read", "all"],
                        "allow_restricted_indices": true
                    }
                ]
            }
            """, """
            {
                "username": "user",
                "has_all_requested": false,
                "cluster": {},
                "index": {
                    ".security-7": {
                        "read_failure_store": false,
                        "read": false,
                        "all": false
                    }
                },
                "application": {}
            }
            """);

        upsertRole("""
            {
              "cluster": ["all"],
              "indices": [
                {
                  "names": [".*"],
                  "privileges": ["read_failure_store"],
                  "allow_restricted_indices": true
                },
                {
                  "names": [".*"],
                  "privileges": ["read"],
                  "allow_restricted_indices": false
                }
              ]
            }
            """, "role");
        apiKeys.remove("user");
        createAndStoreApiKey("user", randomBoolean() ? null : """
            {
                "role": {
                    "cluster": ["all"],
                    "indices": [
                        {
                            "names": [".*"],
                            "privileges": ["read_failure_store"],
                            "allow_restricted_indices": true
                        },
                        {
                            "names": [".*"],
                            "privileges": ["read"],
                            "allow_restricted_indices": false
                        }
                    ]
                }
            }
            """);
        expectHasPrivileges("user", """
            {
                "index": [
                    {
                        "names": [".security-7"],
                        "privileges": ["read_failure_store", "read", "all"],
                        "allow_restricted_indices": true
                    }
                ]
            }
            """, """
            {
                "username": "user",
                "has_all_requested": false,
                "cluster": {},
                "index": {
                    ".security-7": {
                        "read_failure_store": true,
                        "read": false,
                        "all": false
                    }
                },
                "application": {}
            }
            """);

        // invalid payloads with explicit selectors in index patterns
        expectThrows(() -> expectHasPrivileges("user", """
            {
                "index": [
                    {
                        "names": ["test1", "test1::failures"],
                        "privileges": ["read_failure_store", "read", "all"],
                        "allow_restricted_indices": false
                    }
                ]
            }
            """, """
            {}
            """), 400);
        expectThrows(() -> expectHasPrivileges("user", """
            {
                "index": [
                    {
                        "names": ["test1::data"],
                        "privileges": ["read_failure_store", "read", "all"],
                        "allow_restricted_indices": false
                    }
                ]
            }
            """, """
            {}
            """), 400);
        expectThrows(() -> expectHasPrivileges("user", """
            {
                "index": [
                    {
                        "names": ["test1::failures"],
                        "privileges": ["read_failure_store", "read", "all"],
                        "allow_restricted_indices": false
                    }
                ]
            }
            """, """
            {}
            """), 400);
    }

    public void testHasPrivilegesWithApiKeys() throws IOException {
        var user = "user";
        var role = "role";
        createUser(user, PASSWORD, role);
        upsertRole("""
            {
                "cluster": ["all"],
                "indices": [
                    {
                        "names": ["*"],
                        "privileges": ["read_failure_store"]
                    }
                ]
            }
            """, role);

        String apiKey = createApiKey(user, """
            {
                "role": {
                    "cluster": ["all"],
                    "indices": [{"names": ["test1"], "privileges": ["read_failure_store"]}]
                }
            }""");

        expectHasPrivilegesWithApiKey(apiKey, """
            {
                "index": [
                    {
                        "names": ["test1"],
                        "privileges": ["read_failure_store"],
                        "allow_restricted_indices": true
                    },
                    {
                        "names": ["test2"],
                        "privileges": ["read_failure_store"],
                        "allow_restricted_indices": true
                    }
                ]
            }
            """, """
            {
                "username": "user",
                "has_all_requested": false,
                "cluster": {},
                "index": {
                    "test1": {
                        "read_failure_store": true
                    },
                    "test2": {
                        "read_failure_store": false
                    }
                },
                "application": {}
            }
            """);
    }

    public void testRoleWithSelectorInIndexPattern() throws Exception {
        setupDataStream();
        createUser("user", PASSWORD, "role");
        expectThrowsSelectorsNotAllowed(
            () -> upsertRole(
                Strings.format("""
                    {
                      "cluster": ["all"],
                      "indices": [
                        {
                          "names": ["%s"],
                          "privileges": ["%s"]
                        }
                      ]
                    }""", randomFrom("*::failures", "test1::failures", "test1::data", "*::data"), randomFrom("read", "read_failure_store")),
                "role",
                false
            )
        );

        AssertionError bulkFailedError = expectThrows(
            AssertionError.class,
            () -> upsertRole(
                Strings.format("""
                    {
                      "cluster": ["all"],
                      "indices": [
                        {
                          "names": ["%s"],
                          "privileges": ["%s"]
                        }
                      ]
                    }""", randomFrom("*::failures", "test1::failures", "test1::data", "*::data"), randomFrom("read", "read_failure_store")),
                "role",
                true
            )
        );
        assertThat(bulkFailedError.getMessage(), containsString("selectors [::] are not allowed in the index name expression"));

        expectThrowsSelectorsNotAllowed(() -> createApiKey("user", Strings.format("""
            {
                "role": {
                    "cluster": ["all"],
                    "indices": [
                        {
                            "names": ["%s"],
                            "privileges": ["%s"]
                        }
                    ]
                }
            }""", randomFrom("*::failures", "test1::failures", "test1::data", "*::data"), randomFrom("read", "read_failure_store"))));

    }

    public void testFailureStoreAccess() throws Exception {
        List<String> docIds = setupDataStream();
        assertThat(docIds.size(), equalTo(2));
        assertThat(docIds, hasItem("1"));
        String dataDocId = "1";
        String failuresDocId = docIds.stream().filter(id -> false == id.equals(dataDocId)).findFirst().get();

        Tuple<String, String> backingIndices = getSingleDataAndFailureIndices("test1");
        String dataIndexName = backingIndices.v1();
        String failureIndexName = backingIndices.v2();

        createUser(DATA_ACCESS, PASSWORD, DATA_ACCESS);
        upsertRole(Strings.format("""
            {
              "cluster": ["all"],
              "indices": [{"names": ["test*"], "privileges": ["read"]}]
            }"""), DATA_ACCESS);
        createAndStoreApiKey(DATA_ACCESS, randomBoolean() ? null : """
            {
              "role": {
                "cluster": ["all"],
                "indices": [{"names": ["test*"], "privileges": ["read"]}]
              }
            }
            """);

        createUser(STAR_READ_ONLY_ACCESS, PASSWORD, STAR_READ_ONLY_ACCESS);
        upsertRole(Strings.format("""
            {
              "cluster": ["all"],
              "indices": [{"names": ["*"], "privileges": ["read"]}]
            }"""), STAR_READ_ONLY_ACCESS);
        createAndStoreApiKey(STAR_READ_ONLY_ACCESS, randomBoolean() ? null : """
            {
              "role": {
                "cluster": ["all"],
                "indices": [{"names": ["*"], "privileges": ["read"]}]
              }
            }
            """);

        createUser(FAILURE_STORE_ACCESS, PASSWORD, FAILURE_STORE_ACCESS);
        upsertRole(Strings.format("""
            {
              "cluster": ["all"],
              "indices": [{"names": ["test*"], "privileges": ["read_failure_store"]}]
            }"""), FAILURE_STORE_ACCESS);
        createAndStoreApiKey(FAILURE_STORE_ACCESS, randomBoolean() ? null : """
            {
              "role": {
                "cluster": ["all"],
                "indices": [{"names": ["test*"], "privileges": ["read_failure_store"]}]
              }
            }
            """);

        if (randomBoolean()) {
            createUser(BOTH_ACCESS, PASSWORD, BOTH_ACCESS);
            upsertRole(Strings.format("""
                {
                  "cluster": ["all"],
                  "indices": [{"names": ["test*"], "privileges": ["read", "read_failure_store"]}]
                }"""), BOTH_ACCESS);
            createAndStoreApiKey(BOTH_ACCESS, randomBoolean() ? null : """
                {
                  "role": {
                    "cluster": ["all"],
                    "indices": [{"names": ["test*"], "privileges": ["read", "read_failure_store"]}]
                  }
                }
                """);
        } else {
            createUser(BOTH_ACCESS, PASSWORD, DATA_ACCESS, FAILURE_STORE_ACCESS);
            createAndStoreApiKey(BOTH_ACCESS, randomBoolean() ? null : """
                {
                  "role": {
                    "cluster": ["all"],
                    "indices": [{"names": ["test*"], "privileges": ["read", "read_failure_store"]}]
                  }
                }
                """);
        }

        createAndStoreApiKey(WRITE_ACCESS, randomBoolean() ? null : """
            {
              "role": {
                "cluster": ["all"],
                "indices": [{"names": ["test*"], "privileges": ["write", "auto_configure"]}]
              }
            }
            """);

        createUser(BACKING_INDEX_DATA_ACCESS, PASSWORD, BACKING_INDEX_DATA_ACCESS);
        upsertRole(Strings.format("""
            {
              "cluster": ["all"],
              "indices": [{"names": ["%s"], "privileges": ["read"]}]
            }""", dataIndexName), BACKING_INDEX_DATA_ACCESS);
        createAndStoreApiKey(BACKING_INDEX_DATA_ACCESS, null);

        createUser(BACKING_INDEX_FAILURE_ACCESS, PASSWORD, BACKING_INDEX_FAILURE_ACCESS);
        upsertRole(Strings.format("""
            {
              "cluster": ["all"],
              "indices": [{"names": ["%s"], "privileges": ["read_failure_store"]}]
            }""", dataIndexName), BACKING_INDEX_FAILURE_ACCESS);
        createAndStoreApiKey(BACKING_INDEX_FAILURE_ACCESS, null);

        createUser(FAILURE_INDEX_DATA_ACCESS, PASSWORD, FAILURE_INDEX_DATA_ACCESS);
        upsertRole(Strings.format("""
            {
              "cluster": ["all"],
              "indices": [{"names": ["%s"], "privileges": ["read"]}]
            }""", failureIndexName), FAILURE_INDEX_DATA_ACCESS);
        createAndStoreApiKey(FAILURE_INDEX_DATA_ACCESS, null);

        createUser(FAILURE_INDEX_FAILURE_ACCESS, PASSWORD, FAILURE_INDEX_FAILURE_ACCESS);
        upsertRole(Strings.format("""
            {
              "cluster": ["all"],
              "indices": [{"names": ["%s"], "privileges": ["read_failure_store"]}]
            }""", failureIndexName), FAILURE_INDEX_FAILURE_ACCESS);
        createAndStoreApiKey(FAILURE_INDEX_FAILURE_ACCESS, null);

        Request aliasRequest = new Request("POST", "/_aliases");
        aliasRequest.setJsonEntity("""
            {
              "actions": [
                {
                  "add": {
                    "index": "test1",
                    "alias": "test-alias"
                  }
                }
              ]
            }
            """);
        assertOK(adminClient().performRequest(aliasRequest));

        List<String> users = List.of(
            DATA_ACCESS,
            FAILURE_STORE_ACCESS,
            STAR_READ_ONLY_ACCESS,
            BOTH_ACCESS,
            ADMIN_USER,
            BACKING_INDEX_DATA_ACCESS,
            BACKING_INDEX_FAILURE_ACCESS,
            FAILURE_INDEX_DATA_ACCESS,
            FAILURE_INDEX_FAILURE_ACCESS
        );

        // search data
        {
            var request = new Search(randomFrom("test1::data", "test1"));
            for (var user : users) {
                switch (user) {
                    case ADMIN_USER, DATA_ACCESS, STAR_READ_ONLY_ACCESS, BOTH_ACCESS:
                        expectSearch(user, request, dataDocId);
                        expectEsql(user, request, dataDocId);
                        break;
                    case FAILURE_STORE_ACCESS, BACKING_INDEX_DATA_ACCESS, BACKING_INDEX_FAILURE_ACCESS, FAILURE_INDEX_DATA_ACCESS,
                        FAILURE_INDEX_FAILURE_ACCESS:
                        expectSearchThrows(user, request, 403);
                        // also check authz message
                        expectThrowsUnauthorized(
                            user,
                            request,
                            containsString("this action is granted by the index privileges [read,all]")
                        );
                        expectEsqlThrows(user, request, 400);
                        break;
                    default:
                        fail("must cover user: " + user);
                }
            }
        }
        {
            var request = new Search("test1", "?ignore_unavailable=true");
            for (var user : users) {
                switch (user) {
                    case ADMIN_USER, DATA_ACCESS, STAR_READ_ONLY_ACCESS, BOTH_ACCESS:
                        expectSearch(user, request, dataDocId);
                        // es|ql does not support ignore_unavailable
                        break;
                    case FAILURE_STORE_ACCESS, BACKING_INDEX_DATA_ACCESS, BACKING_INDEX_FAILURE_ACCESS, FAILURE_INDEX_DATA_ACCESS,
                        FAILURE_INDEX_FAILURE_ACCESS:
                        // es|ql does not support ignore_unavailable
                        expectSearch(user, request);
                        break;
                    default:
                        fail("must cover user: " + user);
                }
            }
        }
        {
            var request = new Search("test-alias");
            for (var user : users) {
                switch (user) {
                    case ADMIN_USER, DATA_ACCESS, STAR_READ_ONLY_ACCESS, BOTH_ACCESS:
                        expectSearch(user, request, dataDocId);
                        expectEsql(user, request, dataDocId);
                        break;
                    case FAILURE_STORE_ACCESS, BACKING_INDEX_DATA_ACCESS, BACKING_INDEX_FAILURE_ACCESS, FAILURE_INDEX_DATA_ACCESS,
                        FAILURE_INDEX_FAILURE_ACCESS:
                        expectSearchThrows(user, request, 403);
                        expectEsqlThrows(user, request, 400);
                        break;
                    default:
                        fail("must cover user: " + user);
                }
            }
        }
        {
            var request = new Search("test-alias", "?ignore_unavailable=true");
            for (var user : users) {
                switch (user) {
                    case ADMIN_USER, DATA_ACCESS, STAR_READ_ONLY_ACCESS, BOTH_ACCESS:
                        expectSearch(user, request, dataDocId);
                        // es|ql does not support ignore_unavailable
                        break;
                    case FAILURE_STORE_ACCESS, BACKING_INDEX_DATA_ACCESS, BACKING_INDEX_FAILURE_ACCESS, FAILURE_INDEX_DATA_ACCESS,
                        FAILURE_INDEX_FAILURE_ACCESS:
                        expectSearch(user, request);
                        // es|ql does not support ignore_unavailable
                        break;
                    default:
                        fail("must cover user: " + user);
                }
            }
        }
        {
            var request = new Search("test*");
            for (var user : users) {
                switch (user) {
                    case ADMIN_USER, DATA_ACCESS, STAR_READ_ONLY_ACCESS, BOTH_ACCESS:
                        expectSearch(user, request, dataDocId);
                        expectEsql(user, request, dataDocId);
                        break;
                    case FAILURE_STORE_ACCESS, BACKING_INDEX_DATA_ACCESS, BACKING_INDEX_FAILURE_ACCESS, FAILURE_INDEX_DATA_ACCESS,
                        FAILURE_INDEX_FAILURE_ACCESS:
                        expectSearch(user, request);
                        expectEsqlThrows(user, request, 400);
                        break;
                    default:
                        fail("must cover user: " + user);
                }
            }
        }
        {
            var request = new Search("*1");
            for (var user : users) {
                switch (user) {
                    case ADMIN_USER, DATA_ACCESS, STAR_READ_ONLY_ACCESS, BOTH_ACCESS:
                        expectSearch(user, request, dataDocId);
                        expectEsql(user, request, dataDocId);
                        break;
                    case FAILURE_STORE_ACCESS, BACKING_INDEX_DATA_ACCESS, BACKING_INDEX_FAILURE_ACCESS, FAILURE_INDEX_DATA_ACCESS,
                        FAILURE_INDEX_FAILURE_ACCESS:
                        expectSearch(user, request);
                        expectEsqlThrows(user, request, 400);
                        break;
                    default:
                        fail("must cover user: " + user);
                }
            }
        }
        // note expand_wildcards does not include hidden here
        for (var request : List.of(new Search("*"), new Search("_all"), new Search(""))) {
            for (var user : users) {
                switch (user) {
                    case ADMIN_USER, DATA_ACCESS, STAR_READ_ONLY_ACCESS, BOTH_ACCESS:
                        expectSearch(user, request, dataDocId);
                        // es|ql does not support _all or empty target
                        if (request.searchTarget.equals("*")) {
                            expectEsql(user, request, dataDocId);
                        }
                        break;
                    case FAILURE_STORE_ACCESS, BACKING_INDEX_FAILURE_ACCESS, FAILURE_INDEX_FAILURE_ACCESS, FAILURE_INDEX_DATA_ACCESS,
                        BACKING_INDEX_DATA_ACCESS:
                        expectSearch(user, request);
                        expectEsqlThrows(user, request, 400);
                        break;
                    default:
                        fail("must cover user: " + user);
                }
            }
        }
        {
            var request = new Search(".ds*");
            for (var user : users) {
                switch (user) {
                    case ADMIN_USER, DATA_ACCESS, STAR_READ_ONLY_ACCESS, BOTH_ACCESS, BACKING_INDEX_DATA_ACCESS:
                        expectSearch(user, request, dataDocId);
                        expectEsql(user, request, dataDocId);
                        break;
                    case FAILURE_STORE_ACCESS, BACKING_INDEX_FAILURE_ACCESS, FAILURE_INDEX_FAILURE_ACCESS, FAILURE_INDEX_DATA_ACCESS:
                        expectSearch(user, request);
                        expectEsqlThrows(user, request, 400);
                        break;
                    default:
                        fail("must cover user: " + user);
                }
            }
        }
        {
            var request = new Search(dataIndexName);
            for (var user : users) {
                switch (user) {
                    case ADMIN_USER, DATA_ACCESS, STAR_READ_ONLY_ACCESS, BOTH_ACCESS, BACKING_INDEX_DATA_ACCESS:
                        expectSearch(user, request, dataDocId);
                        expectEsql(user, request, dataDocId);
                        break;
                    case FAILURE_STORE_ACCESS, BACKING_INDEX_FAILURE_ACCESS, FAILURE_INDEX_FAILURE_ACCESS, FAILURE_INDEX_DATA_ACCESS:
                        expectSearchThrows(user, request, 403);
                        expectEsqlThrows(user, request, 400);
                        break;
                    default:
                        fail("must cover user: " + user);
                }
            }
        }
        {
            var request = new Search(dataIndexName, "?ignore_unavailable=true");
            for (var user : users) {
                switch (user) {
                    case ADMIN_USER, DATA_ACCESS, STAR_READ_ONLY_ACCESS, BOTH_ACCESS, BACKING_INDEX_DATA_ACCESS:
                        expectSearch(user, request, dataDocId);
                        // es|ql does not support ignore_unavailable
                        break;
                    case FAILURE_STORE_ACCESS, BACKING_INDEX_FAILURE_ACCESS, FAILURE_INDEX_FAILURE_ACCESS, FAILURE_INDEX_DATA_ACCESS:
                        expectSearch(user, request);
                        // es|ql does not support ignore_unavailable
                        break;
                    default:
                        fail("must cover user: " + user);
                }
            }
        }
        {
            var request = new Search("test2");
            for (var user : users) {
                switch (user) {
                    case ADMIN_USER, DATA_ACCESS, STAR_READ_ONLY_ACCESS, BOTH_ACCESS:
                        expectSearchThrows(user, request, 404);
                        expectEsqlThrows(user, request, 400);
                        break;
                    case FAILURE_STORE_ACCESS, BACKING_INDEX_DATA_ACCESS, BACKING_INDEX_FAILURE_ACCESS, FAILURE_INDEX_FAILURE_ACCESS,
                        FAILURE_INDEX_DATA_ACCESS:
                        expectSearchThrows(user, request, 403);
                        expectEsqlThrows(user, request, 400);
                        break;
                    default:
                        fail("must cover user: " + user);
                }
            }
        }
        {
            var request = new Search("test2", "?ignore_unavailable=true");
            for (var user : users) {
                switch (user) {
                    case ADMIN_USER, DATA_ACCESS, STAR_READ_ONLY_ACCESS, BOTH_ACCESS, FAILURE_STORE_ACCESS, BACKING_INDEX_DATA_ACCESS,
                        BACKING_INDEX_FAILURE_ACCESS, FAILURE_INDEX_FAILURE_ACCESS, FAILURE_INDEX_DATA_ACCESS:
                        expectSearch(user, request);
                        // es|ql does not support ignore_unavailable
                        break;
                    default:
                        fail("must cover user: " + user);
                }
            }
        }

        // search failures
        {
            var request = new Search("test1::failures");
            for (var user : users) {
                switch (user) {
                    case DATA_ACCESS, STAR_READ_ONLY_ACCESS, BACKING_INDEX_DATA_ACCESS, BACKING_INDEX_FAILURE_ACCESS,
                        FAILURE_INDEX_FAILURE_ACCESS, FAILURE_INDEX_DATA_ACCESS:
                        expectSearchThrows(user, request, 403);
                        // also check authz message
                        expectThrowsUnauthorized(
                            user,
                            request,
                            containsString(
                                "this action is granted by the index privileges [read,all] for data access, "
                                    + "or by [read_failure_store] for access with the [::failures] selector"
                            )
                        );
                        expectEsqlThrows(user, request, 400);
                        break;
                    case ADMIN_USER, FAILURE_STORE_ACCESS, BOTH_ACCESS:
                        expectSearch(user, request, failuresDocId);
                        expectEsql(user, request, failuresDocId);
                        break;
                    default:
                        fail("must cover user: " + user);
                }
            }
        }
        {
            var request = new Search("test1::failures", "?ignore_unavailable=true");
            for (var user : users) {
                switch (user) {
                    case DATA_ACCESS, STAR_READ_ONLY_ACCESS, BACKING_INDEX_DATA_ACCESS, BACKING_INDEX_FAILURE_ACCESS,
                        FAILURE_INDEX_FAILURE_ACCESS, FAILURE_INDEX_DATA_ACCESS:
                        expectSearch(user, request);
                        // es|ql does not support ignore_unavailable
                        break;
                    case ADMIN_USER, FAILURE_STORE_ACCESS, BOTH_ACCESS:
                        expectSearch(user, request, failuresDocId);
                        // es|ql does not support ignore_unavailable
                        break;
                    default:
                        fail("must cover user: " + user);
                }
            }
        }
        {
            var request = new Search("test-alias::failures");
            for (var user : users) {
                switch (user) {
                    case DATA_ACCESS, STAR_READ_ONLY_ACCESS, BACKING_INDEX_DATA_ACCESS, BACKING_INDEX_FAILURE_ACCESS,
                        FAILURE_INDEX_FAILURE_ACCESS, FAILURE_INDEX_DATA_ACCESS:
                        expectSearchThrows(user, request, 403);
                        expectEsqlThrows(user, request, 400);
                        break;
                    case ADMIN_USER, FAILURE_STORE_ACCESS, BOTH_ACCESS:
                        expectSearch(user, request, failuresDocId);
                        expectEsql(user, request, failuresDocId);
                        break;
                    default:
                        fail("must cover user: " + user);
                }
            }
        }
        {
            var request = new Search("test-alias::failures", "?ignore_unavailable=true");
            for (var user : users) {
                switch (user) {
                    case DATA_ACCESS, STAR_READ_ONLY_ACCESS, BACKING_INDEX_DATA_ACCESS, BACKING_INDEX_FAILURE_ACCESS,
                        FAILURE_INDEX_FAILURE_ACCESS, FAILURE_INDEX_DATA_ACCESS:
                        expectSearch(user, request);
                        // es|ql does not support ignore_unavailable
                        break;
                    case ADMIN_USER, FAILURE_STORE_ACCESS, BOTH_ACCESS:
                        expectSearch(user, request, failuresDocId);
                        // es|ql does not support ignore_unavailable
                        break;
                    default:
                        fail("must cover user: " + user);
                }
            }
        }
        {
            var request = new Search("test*::failures");
            for (var user : users) {
                switch (user) {
                    case DATA_ACCESS, STAR_READ_ONLY_ACCESS, BACKING_INDEX_DATA_ACCESS, BACKING_INDEX_FAILURE_ACCESS,
                        FAILURE_INDEX_FAILURE_ACCESS, FAILURE_INDEX_DATA_ACCESS:
                        expectSearch(user, request);
                        expectEsqlThrows(user, request, 400);
                        break;
                    case ADMIN_USER, FAILURE_STORE_ACCESS, BOTH_ACCESS:
                        expectSearch(user, request, failuresDocId);
                        expectEsql(user, request, failuresDocId);
                        break;
                    default:
                        fail("must cover user: " + user);
                }
            }
        }
        {
            var request = new Search("*1::failures");
            for (var user : users) {
                switch (user) {
                    case DATA_ACCESS, STAR_READ_ONLY_ACCESS, BACKING_INDEX_DATA_ACCESS, BACKING_INDEX_FAILURE_ACCESS,
                        FAILURE_INDEX_FAILURE_ACCESS, FAILURE_INDEX_DATA_ACCESS:
                        expectSearch(user, request);
                        expectEsqlThrows(user, request, 400);
                        break;
                    case ADMIN_USER, FAILURE_STORE_ACCESS, BOTH_ACCESS:
                        expectSearch(user, request, failuresDocId);
                        expectEsql(user, request, failuresDocId);
                        break;
                    default:
                        fail("must cover user: " + user);
                }
            }
        }
        {
            var request = new Search("*::failures");
            for (var user : users) {
                switch (user) {
                    case DATA_ACCESS, STAR_READ_ONLY_ACCESS, BACKING_INDEX_DATA_ACCESS, BACKING_INDEX_FAILURE_ACCESS,
                        FAILURE_INDEX_FAILURE_ACCESS, FAILURE_INDEX_DATA_ACCESS:
                        expectSearch(user, request);
                        expectEsqlThrows(user, request, 400);
                        break;
                    case ADMIN_USER, FAILURE_STORE_ACCESS, BOTH_ACCESS:
                        expectSearch(user, request, failuresDocId);
                        expectEsql(user, request, failuresDocId);
                        break;
                    default:
                        fail("must cover user: " + user);
                }
            }
        }
        {
            var request = new Search(".fs*");
            for (var user : users) {
                switch (user) {
                    case DATA_ACCESS, BACKING_INDEX_DATA_ACCESS, BACKING_INDEX_FAILURE_ACCESS, FAILURE_INDEX_FAILURE_ACCESS:
                        expectSearch(user, request);
                        expectEsqlThrows(user, request, 400);
                        break;
                    case ADMIN_USER, FAILURE_STORE_ACCESS, STAR_READ_ONLY_ACCESS, BOTH_ACCESS, FAILURE_INDEX_DATA_ACCESS:
                        expectSearch(user, request, failuresDocId);
                        expectEsql(user, request, failuresDocId);
                        break;
                    default:
                        fail("must cover user: " + user);
                }
            }
        }
        {
            var request = new Search(failureIndexName);
            for (var user : users) {
                switch (user) {
                    case DATA_ACCESS, BACKING_INDEX_DATA_ACCESS, BACKING_INDEX_FAILURE_ACCESS, FAILURE_INDEX_FAILURE_ACCESS:
                        expectSearchThrows(user, request, 403);
                        expectEsqlThrows(user, request, 400);
                        break;
                    case ADMIN_USER, FAILURE_STORE_ACCESS, STAR_READ_ONLY_ACCESS, BOTH_ACCESS, FAILURE_INDEX_DATA_ACCESS:
                        expectSearch(user, request, failuresDocId);
                        expectEsql(user, request, failuresDocId);
                        break;
                    default:
                        fail("must cover user: " + user);
                }
            }
        }
        {
            var request = new Search(failureIndexName, "?ignore_unavailable=true");
            for (var user : users) {
                switch (user) {
                    case DATA_ACCESS, BACKING_INDEX_DATA_ACCESS, BACKING_INDEX_FAILURE_ACCESS, FAILURE_INDEX_FAILURE_ACCESS:
                        expectSearch(user, request);
                        // es|ql does not support ignore_unavailable
                        break;
                    case ADMIN_USER, FAILURE_STORE_ACCESS, STAR_READ_ONLY_ACCESS, BOTH_ACCESS, FAILURE_INDEX_DATA_ACCESS:
                        expectSearch(user, request, failuresDocId);
                        // es|ql does not support ignore_unavailable
                        break;
                    default:
                        fail("must cover user: " + user);
                }
            }
        }
        {
            var request = new Search(failureIndexName + "::failures");
            for (var user : users) {
                switch (user) {
                    case DATA_ACCESS, STAR_READ_ONLY_ACCESS, BACKING_INDEX_DATA_ACCESS, BACKING_INDEX_FAILURE_ACCESS,
                        FAILURE_INDEX_DATA_ACCESS:
                        expectSearchThrows(user, request, 403);
                        expectEsqlThrows(user, request, 400);
                        break;
                    case FAILURE_STORE_ACCESS, BOTH_ACCESS, ADMIN_USER, FAILURE_INDEX_FAILURE_ACCESS:
                        expectSearchThrows(user, request, 404);
                        expectEsqlThrows(user, request, 400);
                        break;
                    default:
                        fail("must cover user: " + user);
                }
            }
        }
        {
            var request = new Search(failureIndexName + "::failures", "?ignore_unavailable=true");
            for (var user : users) {
                switch (user) {
                    case DATA_ACCESS, FAILURE_STORE_ACCESS, ADMIN_USER, STAR_READ_ONLY_ACCESS, BOTH_ACCESS, BACKING_INDEX_DATA_ACCESS,
                        BACKING_INDEX_FAILURE_ACCESS, FAILURE_INDEX_DATA_ACCESS, FAILURE_INDEX_FAILURE_ACCESS:
                        expectSearch(user, request);
                        // es|ql does not support ignore_unavailable
                        break;
                    default:
                        fail("must cover user: " + user);
                }
            }
        }
        {
            var request = new Search(dataIndexName + "::failures");
            for (var user : users) {
                switch (user) {
                    case STAR_READ_ONLY_ACCESS, BOTH_ACCESS, DATA_ACCESS, FAILURE_STORE_ACCESS, FAILURE_INDEX_DATA_ACCESS,
                        FAILURE_INDEX_FAILURE_ACCESS, BACKING_INDEX_DATA_ACCESS:
                        expectSearchThrows(user, request, 403);
                        expectEsqlThrows(user, request, 400);
                        break;
                    case ADMIN_USER, BACKING_INDEX_FAILURE_ACCESS:
                        expectSearchThrows(user, request, 404);
                        expectEsqlThrows(user, request, 400);
                        break;
                    default:
                        fail("must cover user: " + user);
                }
            }
        }
        {
            var request = new Search(dataIndexName + "::failures", "?ignore_unavailable=true");
            for (var user : users) {
                switch (user) {
                    case DATA_ACCESS, FAILURE_STORE_ACCESS, ADMIN_USER, STAR_READ_ONLY_ACCESS, BOTH_ACCESS, BACKING_INDEX_DATA_ACCESS,
                        BACKING_INDEX_FAILURE_ACCESS, FAILURE_INDEX_DATA_ACCESS, FAILURE_INDEX_FAILURE_ACCESS:
                        expectSearch(user, request);
                        // es|ql does not support ignore_unavailable
                        break;
                    default:
                        fail("must cover user: " + user);
                }
            }
        }
        {
            var request = new Search(".fs*::failures");
            for (var user : users) {
                switch (user) {
                    case DATA_ACCESS, FAILURE_STORE_ACCESS, ADMIN_USER, STAR_READ_ONLY_ACCESS, BOTH_ACCESS, BACKING_INDEX_DATA_ACCESS,
                        BACKING_INDEX_FAILURE_ACCESS, FAILURE_INDEX_DATA_ACCESS, FAILURE_INDEX_FAILURE_ACCESS:
                        expectSearch(user, request);
                        expectEsqlThrows(user, request, 400);
                        break;
                    default:
                        fail("must cover user: " + user);
                }
            }
        }
        {
            var request = new Search(".ds*::failures");
            for (var user : users) {
                switch (user) {
                    case DATA_ACCESS, FAILURE_STORE_ACCESS, ADMIN_USER, STAR_READ_ONLY_ACCESS, BOTH_ACCESS, BACKING_INDEX_DATA_ACCESS,
                        BACKING_INDEX_FAILURE_ACCESS, FAILURE_INDEX_DATA_ACCESS, FAILURE_INDEX_FAILURE_ACCESS:
                        expectSearch(user, request);
                        expectEsqlThrows(user, request, 400);
                        break;
                    default:
                        fail("must cover user: " + user);
                }
            }
        }
        {
            var request = new Search("test2::failures");
            for (var user : users) {
                switch (user) {
                    case DATA_ACCESS, STAR_READ_ONLY_ACCESS, BACKING_INDEX_DATA_ACCESS, BACKING_INDEX_FAILURE_ACCESS,
                        FAILURE_INDEX_DATA_ACCESS, FAILURE_INDEX_FAILURE_ACCESS:
                        expectSearchThrows(user, request, 403);
                        expectEsqlThrows(user, request, 400);
                        break;
                    case ADMIN_USER, FAILURE_STORE_ACCESS, BOTH_ACCESS:
                        expectSearchThrows(user, request, 404);
                        expectEsqlThrows(user, request, 400);
                        break;
                    default:
                        fail("must cover user: " + user);
                }
            }
        }
        {
            var request = new Search("test2::failures", "?ignore_unavailable=true");
            for (var user : users) {
                switch (user) {
                    case ADMIN_USER, DATA_ACCESS, STAR_READ_ONLY_ACCESS, BOTH_ACCESS, FAILURE_STORE_ACCESS, BACKING_INDEX_DATA_ACCESS,
                        BACKING_INDEX_FAILURE_ACCESS, FAILURE_INDEX_DATA_ACCESS, FAILURE_INDEX_FAILURE_ACCESS:
                        expectSearch(user, request);
                        // es|ql does not support ignore_unavailable
                        break;
                    default:
                        fail("must cover user: " + user);
                }
            }
        }

        // mixed access
        {
            var request = new Search("test1,test1::failures");
            for (var user : users) {
                switch (user) {
                    case DATA_ACCESS, FAILURE_STORE_ACCESS, STAR_READ_ONLY_ACCESS:
                        expectSearchThrows(user, request, 403);
                        expectEsqlThrows(user, request, 403);
                        break;
                    case BACKING_INDEX_DATA_ACCESS, BACKING_INDEX_FAILURE_ACCESS, FAILURE_INDEX_DATA_ACCESS, FAILURE_INDEX_FAILURE_ACCESS:
                        expectSearchThrows(user, request, 403);
                        expectEsqlThrows(user, request, 400);
                        break;
                    case ADMIN_USER, BOTH_ACCESS:
                        expectSearch(user, request, dataDocId, failuresDocId);
                        expectEsql(user, request, dataDocId, failuresDocId);
                        break;
                    default:
                        fail("must cover user: " + user);
                }
            }
        }
        {
            var request = new Search("test1,test1::failures", "?ignore_unavailable=true");
            for (var user : users) {
                switch (user) {
                    case DATA_ACCESS, STAR_READ_ONLY_ACCESS:
                        expectSearch(user, request, dataDocId);
                        // es|ql does not support ignore_unavailable
                        break;
                    case FAILURE_STORE_ACCESS:
                        expectSearch(user, request, failuresDocId);
                        // es|ql does not support ignore_unavailable
                        break;
                    case ADMIN_USER, BOTH_ACCESS:
                        expectSearch(user, request, dataDocId, failuresDocId);
                        // es|ql does not support ignore_unavailable
                        break;
                    case BACKING_INDEX_DATA_ACCESS, BACKING_INDEX_FAILURE_ACCESS, FAILURE_INDEX_DATA_ACCESS, FAILURE_INDEX_FAILURE_ACCESS:
                        expectSearch(user, request);
                        // es|ql does not support ignore_unavailable
                        break;
                    default:
                        fail("must cover user: " + user);
                }
            }
        }
        {
            var request = new Search("test1," + failureIndexName);
            for (var user : users) {
                switch (user) {
                    case DATA_ACCESS, FAILURE_STORE_ACCESS, FAILURE_INDEX_DATA_ACCESS:
                        expectSearchThrows(user, request, 403);
                        expectEsqlThrows(user, request, 403);
                        break;
                    case BACKING_INDEX_DATA_ACCESS, FAILURE_INDEX_FAILURE_ACCESS, BACKING_INDEX_FAILURE_ACCESS:
                        expectSearchThrows(user, request, 403);
                        expectEsqlThrows(user, request, 400);
                        break;
                    case ADMIN_USER, BOTH_ACCESS, STAR_READ_ONLY_ACCESS:
                        expectSearch(user, request, dataDocId, failuresDocId);
                        expectEsql(user, request, dataDocId, failuresDocId);
                        break;
                    default:
                        fail("must cover user: " + user);
                }
            }
        }
        {
            var request = new Search("test1," + failureIndexName, "?ignore_unavailable=true");
            for (var user : users) {
                switch (user) {
                    case DATA_ACCESS:
                        expectSearch(user, request, dataDocId);
                        break;
                    case FAILURE_STORE_ACCESS, FAILURE_INDEX_DATA_ACCESS:
                        expectSearch(user, request, failuresDocId);
                        // es|ql does not support ignore_unavailable
                        break;
                    case ADMIN_USER, BOTH_ACCESS, STAR_READ_ONLY_ACCESS:
                        expectSearch(user, request, dataDocId, failuresDocId);
                        // es|ql does not support ignore_unavailable
                        break;
                    case BACKING_INDEX_DATA_ACCESS, BACKING_INDEX_FAILURE_ACCESS, FAILURE_INDEX_FAILURE_ACCESS:
                        expectSearch(user, request);
                        // es|ql does not support ignore_unavailable
                        break;
                    default:
                        fail("must cover user: " + user);
                }
            }
        }
        {
            var request = new Search("test1::failures," + dataIndexName);
            for (var user : users) {
                switch (user) {
                    case DATA_ACCESS, FAILURE_STORE_ACCESS, STAR_READ_ONLY_ACCESS, BACKING_INDEX_DATA_ACCESS:
                        expectSearchThrows(user, request, 403);
                        expectEsqlThrows(user, request, 403);
                        break;
                    case BACKING_INDEX_FAILURE_ACCESS, FAILURE_INDEX_DATA_ACCESS, FAILURE_INDEX_FAILURE_ACCESS:
                        expectSearchThrows(user, request, 403);
                        expectEsqlThrows(user, request, 400);
                        break;
                    case ADMIN_USER, BOTH_ACCESS:
                        expectSearch(user, request, dataDocId, failuresDocId);
                        expectEsql(user, request, dataDocId, failuresDocId);
                        break;
                    default:
                        fail("must cover user: " + user);
                }
            }
        }
        {
            var request = new Search("test1::failures," + dataIndexName, "?ignore_unavailable=true");
            for (var user : users) {
                switch (user) {
                    case DATA_ACCESS, BACKING_INDEX_DATA_ACCESS, STAR_READ_ONLY_ACCESS:
                        expectSearch(user, request, dataDocId);
                        // es|ql does not support ignore_unavailable
                        break;
                    case FAILURE_STORE_ACCESS:
                        expectSearch(user, request, failuresDocId);
                        // es|ql does not support ignore_unavailable
                        break;
                    case ADMIN_USER, BOTH_ACCESS:
                        expectSearch(user, request, dataDocId, failuresDocId);
                        // es|ql does not support ignore_unavailable
                        break;
                    case BACKING_INDEX_FAILURE_ACCESS, FAILURE_INDEX_DATA_ACCESS, FAILURE_INDEX_FAILURE_ACCESS:
                        expectSearch(user, request);
                        // es|ql does not support ignore_unavailable
                        break;
                    default:
                        fail("must cover user: " + user);
                }
            }
        }
        {
            var request = new Search("test1,*::failures");
            for (var user : users) {
                switch (user) {
                    case FAILURE_STORE_ACCESS:
                        expectSearchThrows(user, request, 403);
                        expectEsqlThrows(user, request, 403);
                        break;
                    case BACKING_INDEX_DATA_ACCESS, FAILURE_INDEX_DATA_ACCESS, BACKING_INDEX_FAILURE_ACCESS, FAILURE_INDEX_FAILURE_ACCESS:
                        expectSearchThrows(user, request, 403);
                        expectEsqlThrows(user, request, 400);
                        break;
                    case DATA_ACCESS, STAR_READ_ONLY_ACCESS:
                        expectSearch(user, request, dataDocId);
                        expectEsql(user, request, dataDocId);
                        break;
                    case ADMIN_USER, BOTH_ACCESS:
                        expectSearch(user, request, dataDocId, failuresDocId);
                        expectEsql(user, request, dataDocId, failuresDocId);
                        break;
                    default:
                        fail("must cover user: " + user);
                }
            }
        }
        {
            var request = new Search("test1,*::failures", "?ignore_unavailable=true");
            for (var user : users) {
                switch (user) {
                    case FAILURE_STORE_ACCESS:
                        expectSearch(user, request, failuresDocId);
                        // es|ql does not support ignore_unavailable
                        break;
                    case DATA_ACCESS, STAR_READ_ONLY_ACCESS:
                        expectSearch(user, request, dataDocId);
                        // es|ql does not support ignore_unavailable
                        break;
                    case ADMIN_USER, BOTH_ACCESS:
                        expectSearch(user, request, dataDocId, failuresDocId);
                        // es|ql does not support ignore_unavailable
                        break;
                    case BACKING_INDEX_DATA_ACCESS, BACKING_INDEX_FAILURE_ACCESS, FAILURE_INDEX_DATA_ACCESS, FAILURE_INDEX_FAILURE_ACCESS:
                        expectSearch(user, request);
                        // es|ql does not support ignore_unavailable
                        break;
                    default:
                        fail("must cover user: " + user);
                }
            }
        }
        {
            var request = new Search("test1::failures,*");
            for (var user : users) {
                switch (user) {
                    case FAILURE_STORE_ACCESS:
                        expectSearch(user, request, failuresDocId);
                        expectEsql(user, request, failuresDocId);
                        break;
                    case BACKING_INDEX_DATA_ACCESS, BACKING_INDEX_FAILURE_ACCESS, FAILURE_INDEX_DATA_ACCESS, FAILURE_INDEX_FAILURE_ACCESS:
                        expectSearchThrows(user, request, 403);
                        expectEsqlThrows(user, request, 400);
                        break;
                    case DATA_ACCESS, STAR_READ_ONLY_ACCESS:
                        expectSearchThrows(user, request, 403);
                        expectEsqlThrows(user, request, 403);
                        break;
                    case ADMIN_USER, BOTH_ACCESS:
                        expectSearch(user, request, dataDocId, failuresDocId);
                        expectEsql(user, request, dataDocId, failuresDocId);
                        break;
                    default:
                        fail("must cover user: " + user);
                }
            }
        }
        {
            var request = new Search("test1::failures,*", "?ignore_unavailable=true");
            for (var user : users) {
                switch (user) {
                    case FAILURE_STORE_ACCESS:
                        expectSearch(user, request, failuresDocId);
                        // es|ql does not support ignore_unavailable
                        break;
                    case DATA_ACCESS, STAR_READ_ONLY_ACCESS:
                        expectSearch(user, request, dataDocId);
                        // es|ql does not support ignore_unavailable
                        break;
                    case ADMIN_USER, BOTH_ACCESS:
                        expectSearch(user, request, dataDocId, failuresDocId);
                        // es|ql does not support ignore_unavailable
                        break;
                    case BACKING_INDEX_DATA_ACCESS, BACKING_INDEX_FAILURE_ACCESS, FAILURE_INDEX_DATA_ACCESS, FAILURE_INDEX_FAILURE_ACCESS:
                        expectSearch(user, request);
                        // es|ql does not support ignore_unavailable
                        break;
                    default:
                        fail("must cover user: " + user);
                }
            }
        }
        {
            var request = new Search("*::failures,*");
            for (var user : users) {
                switch (user) {
                    case FAILURE_STORE_ACCESS:
                        expectSearch(user, request, failuresDocId);
                        expectEsql(user, request, failuresDocId);
                        break;
                    case DATA_ACCESS, STAR_READ_ONLY_ACCESS:
                        expectSearch(user, request, dataDocId);
                        expectEsql(user, request, dataDocId);
                        break;
                    case ADMIN_USER, BOTH_ACCESS:
                        expectSearch(user, request, dataDocId, failuresDocId);
                        expectEsql(user, request, dataDocId, failuresDocId);
                        break;
                    case BACKING_INDEX_FAILURE_ACCESS, FAILURE_INDEX_FAILURE_ACCESS, BACKING_INDEX_DATA_ACCESS, FAILURE_INDEX_DATA_ACCESS:
                        expectSearch(user, request);
                        expectEsqlThrows(user, request, 400);
                        break;
                    default:
                        fail("must cover user: " + user);
                }
            }
        }
    }

    public void testModifyingFailureStoreBackingIndices() throws Exception {
        setupDataStream();
        Tuple<String, String> backingIndices = getSingleDataAndFailureIndices("test1");
        String dataIndexName = backingIndices.v1();
        String failureIndexName = backingIndices.v2();

        createUser(MANAGE_ACCESS, PASSWORD, MANAGE_ACCESS);
        createOrUpdateRoleAndApiKey(MANAGE_ACCESS, MANAGE_ACCESS, """
            {
              "cluster": ["all"],
              "indices": [{"names": ["test*"], "privileges": ["manage"]}]
            }""");
        assertOK(addFailureStoreBackingIndex(MANAGE_ACCESS, "test1", failureIndexName));
        assertDataStreamHasDataAndFailureIndices("test1", dataIndexName, failureIndexName);

        // remove the failure index
        assertOK(removeFailureStoreBackingIndex(MANAGE_ACCESS, "test1", failureIndexName));
        assertDataStreamHasNoFailureIndices("test1", dataIndexName);

        // adding it will fail because the user has no direct access to the backing failure index (.fs*)
        expectThrows(() -> addFailureStoreBackingIndex(MANAGE_ACCESS, "test1", failureIndexName), 403);

        // let's change that
        createOrUpdateRoleAndApiKey(MANAGE_ACCESS, MANAGE_ACCESS, """
            {
              "cluster": ["all"],
              "indices": [{"names": ["test*", ".fs*"], "privileges": ["manage"]}]
            }""");

        // adding should succeed now
        assertOK(addFailureStoreBackingIndex(MANAGE_ACCESS, "test1", failureIndexName));
        assertDataStreamHasDataAndFailureIndices("test1", dataIndexName, failureIndexName);

        createUser(MANAGE_FAILURE_STORE_ACCESS, PASSWORD, MANAGE_FAILURE_STORE_ACCESS);
        createOrUpdateRoleAndApiKey(MANAGE_FAILURE_STORE_ACCESS, MANAGE_FAILURE_STORE_ACCESS, """
            {
              "cluster": ["all"],
              "indices": [{"names": ["test*"], "privileges": ["manage_failure_store"]}]
            }""");

        // manage_failure_store can only remove the failure backing index, but not add it
        assertOK(removeFailureStoreBackingIndex(MANAGE_FAILURE_STORE_ACCESS, "test1", failureIndexName));
        assertDataStreamHasNoFailureIndices("test1", dataIndexName);
        expectThrows(() -> addFailureStoreBackingIndex(MANAGE_FAILURE_STORE_ACCESS, "test1", failureIndexName), 403);

        // not even with access to .fs*
        createOrUpdateRoleAndApiKey(MANAGE_FAILURE_STORE_ACCESS, MANAGE_FAILURE_STORE_ACCESS, """
            {
              "cluster": ["all"],
              "indices": [{"names": ["test*", ".fs*"], "privileges": ["manage_failure_store"]}]
            }""");
        expectThrows(() -> addFailureStoreBackingIndex(MANAGE_FAILURE_STORE_ACCESS, "test1", failureIndexName), 403);
    }

    private void assertDataStreamHasDataAndFailureIndices(String dataStreamName, String dataIndexName, String failureIndexName)
        throws IOException {
        Tuple<List<String>, List<String>> indices = getDataAndFailureIndices(dataStreamName);
        assertThat(indices.v1(), containsInAnyOrder(dataIndexName));
        assertThat(indices.v2(), containsInAnyOrder(failureIndexName));
    }

    private void assertDataStreamHasNoFailureIndices(String dataStreamName, String dataIndexName) throws IOException {
        Tuple<List<String>, List<String>> indices = getDataAndFailureIndices(dataStreamName);
        assertThat(indices.v1(), containsInAnyOrder(dataIndexName));
        assertThat(indices.v2(), is(empty()));
    }

    private Response addFailureStoreBackingIndex(String user, String dataStreamName, String failureIndexName) throws IOException {
        return modifyFailureStoreBackingIndex(user, "add_backing_index", dataStreamName, failureIndexName);
    }

    private Response removeFailureStoreBackingIndex(String user, String dataStreamName, String failureIndexName) throws IOException {
        return modifyFailureStoreBackingIndex(user, "remove_backing_index", dataStreamName, failureIndexName);
    }

    private Response modifyFailureStoreBackingIndex(String user, String action, String dataStreamName, String failureIndexName)
        throws IOException {
        Request request = new Request("POST", "/_data_stream/_modify");
        request.setJsonEntity(Strings.format("""
            {
              "actions": [
                {
                  "%s": {
                    "data_stream": "%s",
                    "index": "%s",
                    "failure_store" : true
                  }
                }
              ]
            }
            """, action, dataStreamName, failureIndexName));
        return performRequest(user, request);
    }

    public void testDataStreamApis() throws Exception {
        setupDataStream();
        setupOtherDataStream();

        final String username = "user";
        final String roleName = "role";
        createUser(username, PASSWORD, roleName);
        {
            // manage_failure_store does not grant access to _data_stream APIs
            createOrUpdateRoleAndApiKey(username, roleName, """
                {
                  "cluster": ["all"],
                  "indices": [
                    {
                      "names": ["test1", "other1"],
                      "privileges": ["manage_failure_store"]
                    }
                  ]
                }
                """);

            expectThrows(() -> performRequest(username, new Request("GET", "/_data_stream/test1")), 403);
            expectThrows(() -> performRequest(username, new Request("GET", "/_data_stream/test1/_stats")), 403);
            expectThrows(() -> performRequest(username, new Request("GET", "/_data_stream/test1/_options")), 403);
            expectThrows(() -> performRequest(username, new Request("GET", "/_data_stream/test1/_lifecycle")), 403);
            expectThrows(() -> putDataStreamLifecycle(username, "test1", """
                {
                    "data_retention": "7d"
                }"""), 403);
            expectEmptyDataStreamStats(username, new Request("GET", "/_data_stream/_stats"));
            expectEmptyDataStreamStats(username, new Request("GET", "/_data_stream/" + randomFrom("test*", "*") + "/_stats"));
        }
        {
            createOrUpdateRoleAndApiKey(username, roleName, """
                {
                  "cluster": ["all"],
                  "indices": [
                    {
                      "names": ["test1"],
                      "privileges": ["manage"]
                    }
                  ]
                }
                """);

            expectDataStreamStats(username, new Request("GET", "/_data_stream/_stats"), "test1", 2);
            expectDataStreamStats(
                username,
                new Request("GET", "/_data_stream/" + randomFrom("test1", "test*", "*") + "/_stats"),
                "test1",
                2
            );
            expectDataStreams(username, new Request("GET", "/_data_stream/" + randomFrom("test1", "test*", "*")), "test1");
            putDataStreamLifecycle(username, "test1", """
                {
                    "data_retention": "7d"
                }""");

            var lifecycleResponse = assertOKAndCreateObjectPath(
                performRequest(username, new Request("GET", "/_data_stream/" + randomFrom("test1", "test*", "*") + "/_lifecycle"))
            );
            assertThat(lifecycleResponse.evaluate("data_streams"), iterableWithSize(1));
            assertThat(lifecycleResponse.evaluate("data_streams.0.name"), equalTo("test1"));

            var optionsResponse = assertOKAndCreateObjectPath(
                performRequest(username, new Request("GET", "/_data_stream/" + randomFrom("test1", "test*", "*") + "/_options"))
            );
            assertThat(optionsResponse.evaluate("data_streams"), iterableWithSize(1));
            assertThat(optionsResponse.evaluate("data_streams.0.name"), equalTo("test1"));
        }
    }

    private void putDataStreamLifecycle(String user, String dataStreamName, String lifecyclePolicy) throws IOException {
        Request request = new Request("PUT", "/_data_stream/" + dataStreamName + "/_lifecycle");
        request.setJsonEntity(lifecyclePolicy);
        assertOK(performRequest(user, request));
    }

    private void expectDataStreams(String user, Request dataStreamRequest, String dataStreamName) throws IOException {
        Response response = performRequest(user, dataStreamRequest);
        ObjectPath path = assertOKAndCreateObjectPath(response);
        List<?> dataStreams = path.evaluate("data_streams");
        assertThat(dataStreams.size(), equalTo(1));
        assertThat(path.evaluate("data_streams.0.name"), equalTo(dataStreamName));
    }

    private void expectDataStreamStats(String user, Request statsRequest, String dataStreamName, int backingIndices) throws IOException {
        Response response = performRequest(user, statsRequest);
        ObjectPath path = assertOKAndCreateObjectPath(response);
        assertThat(path.evaluate("data_stream_count"), equalTo(1));
        assertThat(path.evaluate("backing_indices"), equalTo(backingIndices));
        assertThat(path.evaluate("data_streams.0.data_stream"), equalTo(dataStreamName));
    }

    private void expectEmptyDataStreamStats(String user, Request request) throws IOException {
        Response response = performRequest(user, request);
        ObjectPath path = assertOKAndCreateObjectPath(response);
        assertThat(path.evaluate("data_stream_count"), equalTo(0));
        assertThat(path.evaluate("backing_indices"), equalTo(0));
    }

    public void testWatcher() throws Exception {
        setupDataStream();
        setupOtherDataStream();

        final Tuple<String, String> backingIndices = getSingleDataAndFailureIndices("test1");
        final String dataIndexName = backingIndices.v1();
        final String failureIndexName = backingIndices.v2();

        final String watchId = "failures-watch";
        final String username = "user";
        final String roleName = "role";
        createUser(username, PASSWORD, roleName);

        {
            // grant access to the failure store
            createOrUpdateRoleAndApiKey(username, roleName, """
                {
                  "cluster": ["manage_security", "manage_watcher"],
                  "indices": [
                    {
                      "names": ["test1"],
                      "privileges": ["read_failure_store"]
                    }
                  ]
                }
                """);

            // searching the failure store should return only test1 failure indices
            createOrUpdateWatcher(username, watchId, Strings.format("""
                {
                    "trigger": { "schedule": { "interval": "60m"}},
                    "input": {
                        "search": {
                            "request": {
                                "indices": [ "%s" ],
                                "body": {"query": {"match_all": {}}}
                            }
                        }
                    }
                }""", randomFrom("test1::failures", "test*::failures", "*::failures", failureIndexName)));
            executeWatchAndAssertResults(username, watchId, failureIndexName);

            // searching the data should return empty results
            createOrUpdateWatcher(username, watchId, Strings.format("""
                {
                    "trigger": { "schedule": { "interval": "60m"}},
                    "input": {
                        "search": {
                            "request": {
                                "indices": [ "%s" ],
                                "body": {"query": {"match_all": {}}}
                            }
                        }
                    }
                }""", randomFrom(dataIndexName, "*", "test*", "test1", "test1::data")));
            executeWatchAndAssertEmptyResults(username, watchId);
        }

        {
            // remove read_failure_store and add read
            createOrUpdateRoleAndApiKey(username, roleName, """
                {
                  "cluster": ["manage_security", "manage_watcher"],
                  "indices": [
                    {
                      "names": ["test1"],
                      "privileges": ["read"]
                    }
                  ]
                }
                """);

            // searching the failure store should return empty results
            createOrUpdateWatcher(username, watchId, Strings.format("""
                {
                    "trigger": { "schedule": { "interval": "60m"}},
                    "input": {
                        "search": {
                            "request": {
                                "indices": [ "%s" ],
                                "body": {"query": {"match_all": {}}}
                            }
                        }
                    }
                }""", randomFrom("test1::failures", "test*::failures", "*::failures", failureIndexName)));
            executeWatchAndAssertEmptyResults(username, watchId);

            // searching the data should return single result
            createOrUpdateWatcher(username, watchId, Strings.format("""
                {
                    "trigger": { "schedule": { "interval": "60m"}},
                    "input": {
                        "search": {
                            "request": {
                                "indices": [ "%s" ],
                                "body": {"query": {"match_all": {}}}
                            }
                        }
                    }
                }""", randomFrom("*", "test*", "test1", "test1::data", dataIndexName)));
            executeWatchAndAssertResults(username, watchId, dataIndexName);
        }
    }

    private void executeWatchAndAssertResults(String user, String watchId, final String... expectedIndices) throws IOException {
        Request request = new Request("POST", "_watcher/watch/" + watchId + "/_execute");
        Response response = performRequest(user, request);
        ObjectPath path = assertOKAndCreateObjectPath(response);
        assertThat(path.evaluate("watch_record.user"), equalTo(user));
        assertThat(path.evaluate("watch_record.state"), equalTo("executed"));
        assertThat(path.evaluate("watch_record.result.input.status"), equalTo("success"));
        assertThat(path.evaluate("watch_record.result.input.payload.hits.total"), equalTo(expectedIndices.length));
        List<Map<String, ?>> hits = path.evaluate("watch_record.result.input.payload.hits.hits");
        hits.stream().map(hit -> hit.get("_index")).forEach(index -> { assertThat(index, is(in(expectedIndices))); });
    }

    private void executeWatchAndAssertEmptyResults(String user, String watchId) throws IOException {
        Request request = new Request("POST", "_watcher/watch/" + watchId + "/_execute");
        Response response = performRequest(user, request);
        ObjectPath path = assertOKAndCreateObjectPath(response);
        assertThat(path.evaluate("watch_record.user"), equalTo(user));
        assertThat(path.evaluate("watch_record.state"), equalTo("executed"));
        assertThat(path.evaluate("watch_record.result.input.status"), equalTo("success"));
        assertThat(path.evaluate("watch_record.result.input.payload.hits.total"), equalTo(0));
        List<Map<String, ?>> hits = path.evaluate("watch_record.result.input.payload.hits.hits");
        assertThat(hits.size(), equalTo(0));
    }

    private void createOrUpdateWatcher(String user, String watchId, String watch) throws IOException {
        Request request = new Request("PUT", "/_watcher/watch/" + watchId);
        request.setJsonEntity(watch);
        assertOK(performRequest(user, request));
    }

    public void testAliasBasedAccess() throws Exception {
        List<String> docIds = setupDataStream();
        assertThat(docIds.size(), equalTo(2));
        assertThat(docIds, hasItem("1"));
        String dataDocId = "1";
        String failuresDocId = docIds.stream().filter(id -> false == id.equals(dataDocId)).findFirst().get();

        List<String> otherDocIds = setupOtherDataStream();
        assertThat(otherDocIds.size(), equalTo(2));
        assertThat(otherDocIds, hasItem("3"));
        String otherDataDocId = "3";
        String otherFailuresDocId = otherDocIds.stream().filter(id -> false == id.equals(otherDataDocId)).findFirst().get();

        final Tuple<String, String> backingIndices = getSingleDataAndFailureIndices("test1");
        final String dataIndexName = backingIndices.v1();
        final String failureIndexName = backingIndices.v2();

        final String aliasName = "my-alias";
        final String username = "user";
        final String roleName = "role";

        createUser(username, PASSWORD, roleName);
        // manage is required to add the alias to the data stream
        createOrUpdateRoleAndApiKey(username, roleName, Strings.format("""
            {
              "cluster": ["all"],
              "indices": [
                {
                  "names": ["test1", "%s", "other1"],
                  "privileges": ["manage"]
                }
              ]
            }
            """, aliasName));

        addAlias(username, "test1", aliasName, "");
        addAlias(username, "other1", aliasName, "");
        assertThat(fetchAliases(username, "test1"), containsInAnyOrder(aliasName));
        expectSearchThrows(username, new Search(randomFrom(aliasName + "::data", aliasName)), 403);
        expectSearchThrows(username, new Search(randomFrom(aliasName + "::failures")), 403);

        createOrUpdateRoleAndApiKey(username, roleName, Strings.format("""
            {
              "cluster": ["all"],
              "indices": [
                {
                  "names": ["%s"],
                  "privileges": ["read_failure_store"]
                }
              ]
            }
            """, aliasName));
        expectSearch(username, new Search(aliasName + "::failures"), failuresDocId, otherFailuresDocId);
        expectSearchThrows(username, new Search(randomFrom(aliasName + "::data", aliasName, dataIndexName, failureIndexName)), 403);

        createOrUpdateRoleAndApiKey(username, roleName, Strings.format("""
            {
              "cluster": ["all"],
              "indices": [
                {
                  "names": ["%s"],
                  "privileges": ["read"]
                }
              ]
            }
            """, aliasName));
        expectSearch(username, new Search(randomFrom(aliasName + "::data")), dataDocId, otherDataDocId);
        expectSearchThrows(username, new Search(aliasName + "::failures"), 403);

        expectThrows(() -> removeAlias(username, "test1", aliasName), 403);
        createOrUpdateRoleAndApiKey(username, roleName, Strings.format("""
            {
              "cluster": ["all"],
              "indices": [
                {
                  "names": ["test1", "%s", "other1"],
                  "privileges": ["manage"]
                }
              ]
            }
            """, aliasName));
        removeAlias(username, "test1", aliasName);
        removeAlias(username, "other1", aliasName);

        final String filteredAliasName = "my-filtered-alias";
        createOrUpdateRoleAndApiKey(username, roleName, Strings.format("""
            {
              "cluster": ["all"],
              "indices": [
                {
                  "names": ["test1", "%s", "other1"],
                  "privileges": ["manage"]
                }
              ]
            }
            """, filteredAliasName));
        addAlias(username, "test1", filteredAliasName, """
            {
              "term": {
                "document.source.name": "jack"
              }
            }
            """);
        addAlias(username, "other1", filteredAliasName, """
            {
              "term": {
                "document.source.name": "jack"
              }
            }
            """);
        assertThat(fetchAliases(username, "test1"), containsInAnyOrder(filteredAliasName));
        assertThat(fetchAliases(username, "other1"), containsInAnyOrder(filteredAliasName));

        createOrUpdateRoleAndApiKey(username, roleName, Strings.format("""
            {
              "cluster": ["all"],
              "indices": [
                {
                  "names": ["%s"],
                  "privileges": ["read", "read_failure_store"]
                }
              ]
            }
            """, filteredAliasName));

        expectSearch(username, new Search(randomFrom(filteredAliasName + "::data", filteredAliasName)));
        // the alias filter is not applied to the failure store
        expectSearch(username, new Search(filteredAliasName + "::failures"), failuresDocId, otherFailuresDocId);
    }

    private void createOrUpdateRoleAndApiKey(String username, String roleName, String roleDescriptor) throws IOException {
        upsertRole(roleDescriptor, roleName);
        createOrUpdateApiKey(username, randomBoolean() ? null : Strings.format("""
            {
              "%s": %s
            }
            """, roleName, roleDescriptor));
    }

    private void addAlias(String user, String dataStream, String alias, String filter) throws IOException {
        aliasAction(user, "add", dataStream, alias, filter);
    }

    private void removeAlias(String user, String dataStream, String alias) throws IOException {
        aliasAction(user, "remove", dataStream, alias, "");
    }

    private void aliasAction(String user, String action, String dataStream, String alias, String filter) throws IOException {
        Request request = new Request("POST", "/_aliases");
        if (filter == null || filter.isEmpty()) {
            request.setJsonEntity(Strings.format("""
                {
                  "actions": [
                    {
                      "%s": {
                        "index": "%s",
                        "alias": "%s"
                      }
                    }
                  ]
                }
                """, action, dataStream, alias));
        } else {
            request.setJsonEntity(Strings.format("""
                {
                  "actions": [
                    {
                      "%s": {
                        "index": "%s",
                        "alias": "%s",
                        "filter": %s
                      }
                    }
                  ]
                }
                """, action, dataStream, alias, filter));
        }
        Response response = performRequestMaybeUsingApiKey(user, request);
        var path = assertOKAndCreateObjectPath(response);
        assertThat(path.evaluate("acknowledged"), is(true));
        assertThat(path.evaluate("errors"), is(false));

    }

    private Set<String> fetchAliases(String user, String dataStream) throws IOException {
        Response response = performRequestMaybeUsingApiKey(user, new Request("GET", dataStream + "/_alias"));
        ObjectPath path = assertOKAndCreateObjectPath(response);
        Map<String, Object> aliases = path.evaluate(dataStream + ".aliases");
        return aliases.keySet();
    }

    public void testPatternExclusions() throws Exception {
        List<String> docIds = setupDataStream();
        assertThat(docIds.size(), equalTo(2));
        assertThat(docIds, hasItem("1"));
        String dataDocId = "1";
        String failuresDocId = docIds.stream().filter(id -> false == id.equals(dataDocId)).findFirst().get();

        List<String> otherDocIds = setupOtherDataStream();
        assertThat(otherDocIds.size(), equalTo(2));
        assertThat(otherDocIds, hasItem("3"));
        String otherDataDocId = "3";
        String otherFailuresDocId = otherDocIds.stream().filter(id -> false == id.equals(otherDataDocId)).findFirst().get();

        createUser("user", PASSWORD, "role");
        upsertRole("""
            {
              "cluster": ["all"],
              "indices": [
                {
                  "names": ["test*", "other*"],
                  "privileges": ["read", "read_failure_store"]
                }
              ]
            }
            """, "role");
        createAndStoreApiKey("user", randomBoolean() ? null : """
            {
              "role": {
                "cluster": ["all"],
                "indices": [
                  {
                    "names": ["*"],
                    "privileges": ["read", "read_failure_store"]
                  }
                ]
              }
            }
            """);

        // no exclusion -> should return two failure docs
        expectSearch("user", new Search("*::failures"), failuresDocId, otherFailuresDocId);
        expectSearch("user", new Search("*::failures,-other*::failures"), failuresDocId);
    }

    @SuppressWarnings("unchecked")
    private void expectEsql(String user, Search search, String... docIds) throws Exception {
        var response = performRequestMaybeUsingApiKey(user, search.toEsqlRequest());
        Map<String, Object> responseAsMap = entityAsMap(response);
        List<?> columns = (List<?>) responseAsMap.get("columns");
        List<?> values = (List<?>) responseAsMap.get("values");
        assertEquals(1, columns.size());
        assertEquals(docIds.length, values.size());
        List<String> flatList = values.stream()
            .flatMap(innerList -> innerList instanceof List ? ((List<String>) innerList).stream() : Stream.empty())
            .collect(Collectors.toList());
        assertThat(flatList, containsInAnyOrder(docIds));
    }

    private void expectEsqlThrows(String user, Search search, int statusCode) {
        expectThrows(() -> performRequestMaybeUsingApiKey(user, search.toEsqlRequest()), statusCode);
    }

    public void testWriteAndManageOperations() throws IOException {
        setupDataStream();
        Tuple<String, String> backingIndices = getSingleDataAndFailureIndices("test1");
        String dataIndexName = backingIndices.v1();
        String failureIndexName = backingIndices.v2();

        createUser(MANAGE_ACCESS, PASSWORD, MANAGE_ACCESS);
        upsertRole(Strings.format("""
            {
              "cluster": ["all"],
              "indices": [{"names": ["test*"], "privileges": ["manage"]}]
            }"""), MANAGE_ACCESS);
        createAndStoreApiKey(MANAGE_ACCESS, randomBoolean() ? null : """
            {
              "role": {
                "cluster": ["all"],
                "indices": [{"names": ["test*"], "privileges": ["manage"]}]
              }
            }
            """);

        createUser(MANAGE_FAILURE_STORE_ACCESS, PASSWORD, MANAGE_FAILURE_STORE_ACCESS);
        upsertRole(Strings.format("""
            {
              "cluster": ["all"],
              "indices": [{"names": ["test*"], "privileges": ["manage_failure_store"]}]
            }"""), MANAGE_FAILURE_STORE_ACCESS);
        createAndStoreApiKey(MANAGE_FAILURE_STORE_ACCESS, randomBoolean() ? null : """
            {
              "role": {
                "cluster": ["all"],
                "indices": [{"names": ["test*"], "privileges": ["manage_failure_store"]}]
              }
            }
            """);

        createUser(MANAGE_DATA_STREAM_LIFECYCLE, PASSWORD, MANAGE_DATA_STREAM_LIFECYCLE);
        upsertRole(Strings.format("""
            {
              "cluster": ["all"],
              "indices": [{"names": ["test*"], "privileges": ["manage_data_stream_lifecycle"]}]
            }"""), MANAGE_DATA_STREAM_LIFECYCLE);
        createAndStoreApiKey(MANAGE_DATA_STREAM_LIFECYCLE, randomBoolean() ? null : """
            {
              "role": {
                "cluster": ["all"],
                "indices": [{"names": ["test*"], "privileges": ["manage_data_stream_lifecycle"]}]
              }
            }
            """);

        // explain lifecycle API with and without failures selector is granted by manage
        assertOK(performRequest(MANAGE_ACCESS, new Request("GET", "test1/_lifecycle/explain")));
        assertOK(performRequest(MANAGE_ACCESS, new Request("GET", "test1::failures/_lifecycle/explain")));
        assertOK(performRequest(MANAGE_ACCESS, new Request("GET", failureIndexName + "/_lifecycle/explain")));
        assertOK(performRequest(MANAGE_ACCESS, new Request("GET", dataIndexName + "/_lifecycle/explain")));

        assertOK(performRequest(MANAGE_DATA_STREAM_LIFECYCLE, new Request("GET", "test1/_lifecycle/explain")));
        assertOK(performRequest(MANAGE_DATA_STREAM_LIFECYCLE, new Request("GET", "test1::failures/_lifecycle/explain")));
        assertOK(performRequest(MANAGE_DATA_STREAM_LIFECYCLE, new Request("GET", failureIndexName + "/_lifecycle/explain")));
        assertOK(performRequest(MANAGE_DATA_STREAM_LIFECYCLE, new Request("GET", dataIndexName + "/_lifecycle/explain")));

        // explain lifecycle API is granted by manage_failure_store only for failures selector
        expectThrows(() -> performRequest(MANAGE_FAILURE_STORE_ACCESS, new Request("GET", "test1/_lifecycle/explain")), 403);
        assertOK(performRequest(MANAGE_FAILURE_STORE_ACCESS, new Request("GET", "test1::failures/_lifecycle/explain")));
        assertOK(performRequest(MANAGE_FAILURE_STORE_ACCESS, new Request("GET", failureIndexName + "/_lifecycle/explain")));
        expectThrows(() -> performRequest(MANAGE_FAILURE_STORE_ACCESS, new Request("GET", dataIndexName + "/_lifecycle/explain")), 403);

        // user with manage access to data stream can delete failure index because manage grants access to both data and failures
        expectThrows(() -> deleteIndex(MANAGE_ACCESS, failureIndexName), 400);
        expectThrows(() -> deleteIndex(MANAGE_ACCESS, dataIndexName), 400);

        // manage_failure_store user COULD delete failure index (not valid because it's a write index, but allowed security-wise)
        expectThrows(() -> deleteIndex(MANAGE_FAILURE_STORE_ACCESS, failureIndexName), 400);
        expectThrows(() -> deleteIndex(MANAGE_FAILURE_STORE_ACCESS, dataIndexName), 403);
        expectThrows(() -> deleteDataStream(MANAGE_FAILURE_STORE_ACCESS, dataIndexName), 403);

        expectThrows(() -> deleteDataStream(MANAGE_FAILURE_STORE_ACCESS, "test1"), 403);
        // selectors aren't supported for deletes so we get a 403
        expectThrowsBadRequest(
            () -> deleteDataStream(MANAGE_FAILURE_STORE_ACCESS, "test1::failures"),
            containsString("Index component selectors are not supported in this context but found selector in expression [test1::failures]")
        );

        // manage user can delete data stream
        deleteDataStream(MANAGE_ACCESS, "test1");

        // deleting data stream deletes everything, including failure index
        expectThrows(() -> adminClient().performRequest(new Request("GET", "/test1/_search")), 404);
        expectThrows(() -> adminClient().performRequest(new Request("GET", "/" + dataIndexName + "/_search")), 404);
        expectThrows(() -> adminClient().performRequest(new Request("GET", "/test1::failures/_search")), 404);
        expectThrows(() -> adminClient().performRequest(new Request("GET", "/" + failureIndexName + "/_search")), 404);
    }

    public void testFailureStoreAccessWithApiKeys() throws Exception {
        List<String> docIds = setupDataStream();
        assertThat(docIds.size(), equalTo(2));
        assertThat(docIds, hasItem("1"));
        String dataDocId = "1";
        String failuresDocId = docIds.stream().filter(id -> false == id.equals(dataDocId)).findFirst().get();

        Tuple<String, String> backingIndices = getSingleDataAndFailureIndices("test1");
        String dataIndexName = backingIndices.v1();
        String failureIndexName = backingIndices.v2();

        var user = "user";
        var role = "role";
        createUser(user, PASSWORD, role);
        upsertRole("""
            {
                "cluster": ["all"],
                "indices": [
                    {
                        "names": ["*"],
                        "privileges": ["read_failure_store"]
                    }
                ]
            }
            """, role);

        String apiKey = createApiKey(user, """
            {
                "role": {
                    "cluster": ["all"],
                    "indices": [{"names": ["test1"], "privileges": ["read_failure_store"]}]
                }
            }""");

        expectSearchWithApiKey(apiKey, new Search("test1::failures"), failuresDocId);
        expectSearchWithApiKey(apiKey, new Search(failureIndexName), failuresDocId);
        expectThrowsWithApiKey(apiKey, new Search(dataIndexName), 403);
        expectThrowsWithApiKey(apiKey, new Search("test1"), 403);

        apiKey = createApiKey(user, """
            {
                "role": {
                    "cluster": ["all"],
                    "indices": [{"names": ["test1"], "privileges": ["read_failure_store", "read"]}]
                }
            }""");

        expectSearchWithApiKey(apiKey, new Search("test1::failures"), failuresDocId);
        expectSearchWithApiKey(apiKey, new Search(failureIndexName), failuresDocId);
        expectThrowsWithApiKey(apiKey, new Search(dataIndexName), 403);
        expectThrowsWithApiKey(apiKey, new Search("test1"), 403);

        apiKey = createApiKey(user, """
            {
                "role": {
                    "cluster": ["all"],
                    "indices": [{"names": ["test2"], "privileges": ["read_failure_store", "read"]}]
                }
            }""");

        expectThrowsWithApiKey(apiKey, new Search("test1::failures"), 403);
        expectThrowsWithApiKey(apiKey, new Search(failureIndexName), 403);
        expectThrowsWithApiKey(apiKey, new Search(dataIndexName), 403);
        expectThrowsWithApiKey(apiKey, new Search("test1"), 403);

        apiKey = createApiKey(user, """
            {
                "role": {
                    "cluster": ["all"],
                    "indices": [
                        {"names": ["test1"], "privileges": ["read_failure_store"]},
                        {"names": ["*"], "privileges": ["read"]}
                    ]
                }
            }""");

        expectSearchWithApiKey(apiKey, new Search("test1::failures"), failuresDocId);
        expectSearchWithApiKey(apiKey, new Search(failureIndexName), failuresDocId);
        expectThrowsWithApiKey(apiKey, new Search(dataIndexName), 403);
        expectThrowsWithApiKey(apiKey, new Search("test1"), 403);

        apiKey = createApiKey(user, """
            {
                "role": {
                    "cluster": ["all"],
                    "indices": [
                        {"names": ["*"], "privileges": ["read"]}
                    ]
                }
            }""");

        expectThrowsWithApiKey(apiKey, new Search("test1::failures"), 403);
        // funky but correct: assigned role descriptors grant direct access to failure index, limited-by to failure store
        expectSearchWithApiKey(apiKey, new Search(failureIndexName), failuresDocId);
        expectThrowsWithApiKey(apiKey, new Search(dataIndexName), 403);
        expectThrowsWithApiKey(apiKey, new Search("test1"), 403);

        upsertRole("""
            {
                "cluster": ["all"],
                "indices": [
                    {
                        "names": ["*"],
                        "privileges": ["read"]
                    }
                ]
            }
            """, role);
        apiKey = createApiKey(user, """
            {
                "role": {
                    "cluster": ["all"],
                    "indices": [
                        {"names": ["test1"], "privileges": ["read_failure_store"]}
                    ]
                }
            }""");
        expectThrowsWithApiKey(apiKey, new Search("test1::failures"), 403);
        // funky but correct: limited-by role descriptors grant direct access to failure index, assigned to failure store
        expectSearchWithApiKey(apiKey, new Search(failureIndexName), failuresDocId);
        expectThrowsWithApiKey(apiKey, new Search(dataIndexName), 403);
        expectThrowsWithApiKey(apiKey, new Search("test1"), 403);
    }

    public void testFieldCapabilities() throws Exception {
        setupDataStream();

        final Tuple<String, String> backingIndices = getSingleDataAndFailureIndices("test1");
        final String dataIndexName = backingIndices.v1();
        final String failureIndexName = backingIndices.v2();

        createUser("user", PASSWORD, "role");
        upsertRole("""
            {
              "cluster": ["all"],
              "indices": [
                {
                  "names": ["test*"],
                  "privileges": ["read"]
                }
              ]
            }""", "role");

        {
            expectThrows(() -> performRequest("user", new Request("POST", "/test1::failures/_field_caps?fields=name")), 403);
            assertFieldCapsResponseContainsIndexAndFields(
                performRequest("user", new Request("POST", Strings.format("/%s/_field_caps?fields=name", "test1"))),
                dataIndexName,
                Set.of("name")
            );
        }

        upsertRole("""
            {
              "cluster": ["all"],
              "indices": [
                {
                  "names": ["test*"],
                  "privileges": ["read_failure_store"]
                }
              ]
            }""", "role");

        {
            expectThrows(() -> performRequest("user", new Request("POST", "/test1/_field_caps?fields=name")), 403);
            assertFieldCapsResponseContainsIndexAndFields(
                performRequest("user", new Request("POST", "/test1::failures/_field_caps?fields=error.*")),
                failureIndexName,
                Set.of(
                    "error.message",
                    "error.pipeline_trace",
                    "error.processor_type",
                    "error.type",
                    "error.processor_tag",
                    "error.pipeline",
                    "error.stack_trace",
                    "error"
                )
            );
        }

        upsertRole("""
            {
              "cluster": ["all"],
              "indices": [
                {
                  "names": ["test*"],
                  "privileges": ["read_failure_store"],
                  "field_security": {
                      "grant": ["error*"],
                      "except": ["error.message"]
                  }
                }
              ]
            }""", "role");
        {
            expectThrows(() -> performRequest("user", new Request("POST", "/test1/_field_caps?fields=name")), 403);
            assertFieldCapsResponseContainsIndexAndFields(
                performRequest("user", new Request("POST", "/test1::failures/_field_caps?fields=error.*")),
                failureIndexName,
                Set.of(
                    "error.pipeline_trace",
                    "error.processor_type",
                    "error.type",
                    "error.processor_tag",
                    "error.pipeline",
                    "error.stack_trace",
                    "error"
                )
            );
        }

        upsertRole("""
            {
              "cluster": ["all"],
              "indices": [
                {
                  "names": ["test*"],
                  "privileges": ["read_failure_store", "read"],
                  "field_security": {
                      "grant": ["error*", "name"],
                      "except": ["error.type"]
                  }
                }
              ]
            }""", "role");
        {
            assertFieldCapsResponseContainsIndexAndFields(
                performRequest("user", new Request("POST", "/test1/_field_caps?fields=name,age,email")),
                dataIndexName,
                Set.of("name")
            );
            assertFieldCapsResponseContainsIndexAndFields(
                performRequest("user", new Request("POST", "/test1::failures/_field_caps?fields=error.*")),
                failureIndexName,
                Set.of(
                    "error.pipeline_trace",
                    "error.processor_type",
                    "error.message",
                    "error.processor_tag",
                    "error.pipeline",
                    "error.stack_trace",
                    "error"
                )
            );
        }

        upsertRole("""
            {
              "cluster": ["all"],
              "indices": [
                {
                  "names": ["other*"],
                  "privileges": ["read_failure_store", "read"]
                }
              ]
            }""", "role");
        expectThrows(() -> performRequest("user", new Request("POST", "/test1/_field_caps?fields=name")), 403);
        expectThrows(() -> performRequest("user", new Request("POST", "/test1::failures/_field_caps?fields=name")), 403);
    }

    private void assertFieldCapsResponseContainsIndexAndFields(Response fieldCapsResponse, String indexName, Set<String> expectedFields)
        throws IOException {
        assertOK(fieldCapsResponse);
        ObjectPath objectPath = ObjectPath.createFromResponse(fieldCapsResponse);

        List<String> indices = objectPath.evaluate("indices");
        assertThat(indices, containsInAnyOrder(indexName));

        Map<String, Object> fields = objectPath.evaluate("fields");
        assertThat(fields.keySet(), containsInAnyOrder(expectedFields.toArray()));
    }

    public void testPit() throws Exception {
        List<String> docIds = setupDataStream();
        String dataDocId = "1";
        String failuresDocId = docIds.stream().filter(id -> false == id.equals(dataDocId)).findFirst().get();

        createUser("user", PASSWORD, "role");
        upsertRole("""
            {
              "cluster": ["all"],
              "indices": [
                {
                  "names": ["test*"],
                  "privileges": ["read"]
                }
              ]
            }""", "role");

        {
            expectThrows(
                () -> performRequest("user", new Request("POST", Strings.format("/%s/_pit?keep_alive=1m", "test1::failures"))),
                403
            );
            Response pitResponse = performRequest("user", new Request("POST", Strings.format("/%s/_pit?keep_alive=1m", "test1")));
            assertOK(pitResponse);
            String pitId = ObjectPath.createFromResponse(pitResponse).evaluate("id");
            assertThat(pitId, notNullValue());

            var searchRequest = new Request("POST", "/_search");
            searchRequest.setJsonEntity(Strings.format("""
                {
                    "pit": {
                        "id": "%s"
                    }
                }
                """, pitId));
            Response searchResponse = performRequest("user", searchRequest);
            expectSearch(searchResponse, dataDocId);
        }

        upsertRole("""
            {
              "cluster": ["all"],
              "indices": [
                {
                  "names": ["test*"],
                  "privileges": ["read_failure_store"]
                }
              ]
            }""", "role");

        {
            expectThrows(() -> performRequest("user", new Request("POST", Strings.format("/%s/_pit?keep_alive=1m", "test1"))), 403);
            Response pitResponse = performRequest("user", new Request("POST", Strings.format("/%s/_pit?keep_alive=1m", "test1::failures")));
            assertOK(pitResponse);
            String pitId = ObjectPath.createFromResponse(pitResponse).evaluate("id");
            assertThat(pitId, notNullValue());

            var searchRequest = new Request("POST", "/_search");
            searchRequest.setJsonEntity(Strings.format("""
                {
                    "pit": {
                        "id": "%s"
                    }
                }
                """, pitId));
            Response searchResponse = performRequest("user", searchRequest);
            expectSearch(searchResponse, failuresDocId);
        }
    }

    public void testScroll() throws Exception {
        List<String> docIds = setupDataStream();
        String dataDocId = "1";
        String failuresDocId = docIds.stream().filter(id -> false == id.equals(dataDocId)).findFirst().get();

        createUser("user", PASSWORD, "role");
        upsertRole("""
            {
              "cluster": ["all"],
              "indices": [
                {
                  "names": ["test*"],
                  "privileges": ["read"]
                }
              ]
            }""", "role");

        {
            // user has no access to failure store, searching failures should not work
            expectThrows(
                () -> performRequest("user", new Request("POST", Strings.format("/%s/_search?scroll=1m", "test1::failures"))),
                403
            );

            // searching data should work
            final String scrollId = performScrollSearchRequestAndAssertDocs("test1", dataDocId);

            // further searches with scroll_id should work, but won't return any more hits
            assertSearchHasNoHits(performScrollSearchRequest("user", scrollId));

            deleteScroll(scrollId);
        }

        upsertRole("""
            {
              "cluster": ["all"],
              "indices": [
                {
                  "names": ["test*"],
                  "privileges": ["read_failure_store"]
                }
              ]
            }""", "role");

        {
            // user has only read access to failure store, searching data should fail
            expectThrows(() -> performRequest("user", new Request("POST", Strings.format("/%s/_search?scroll=1m", "test1"))), 403);

            // searching failure store should work
            final String scrollId = performScrollSearchRequestAndAssertDocs("test1::failures", failuresDocId);

            // further searches with scroll_id should work, but won't return any more hits
            assertSearchHasNoHits(performScrollSearchRequest("user", scrollId));

            deleteScroll(scrollId);
        }
    }

    public void testDlsFls() throws Exception {
        setupDataStream();

        Tuple<String, String> backingIndices = getSingleDataAndFailureIndices("test1");
        String dataIndexName = backingIndices.v1();
        String failureIndexName = backingIndices.v2();

        String user = "user";
        String role = "role";
        createUser(user, PASSWORD, role);
        upsertRole("""
            {
                 "cluster": ["all"],
                 "indices": [
                     {
                        "names": ["test*"],
                        "privileges": ["read", "read_failure_store"],
                        "field_security": {
                            "grant": ["@timestamp", "age"]
                        }
                     }
                 ]
             }""", role);

        // FLS applies to regular data stream
        assertSearchResponseContainsExpectedIndicesAndFields(
            performRequest(user, new Search(randomFrom("test1", "test1::data")).toSearchRequest()),
            Map.of(dataIndexName, Set.of("@timestamp", "age"))
        );

        // FLS applies to failure store
        assertSearchResponseContainsExpectedIndicesAndFields(
            performRequest(user, new Search("test1::failures").toSearchRequest()),
            Map.of(failureIndexName, Set.of("@timestamp"))
        );

        upsertRole(Strings.format("""
            {
                 "cluster": ["all"],
                 "indices": [
                     {
                        "names": ["%s"],
                        "privileges": ["read"],
                        "field_security": {
                            "grant": ["@timestamp", "age"]
                        }
                     },
                     {
                        "names": ["test*"],
                        "privileges": ["read_failure_store"],
                        "field_security": {
                            "grant": ["error.type", "error.message"]
                        }
                     }
                 ]
             }""", "test1"), role);

        // FLS applies to regular data stream
        assertSearchResponseContainsExpectedIndicesAndFields(
            performRequest(user, new Search(randomFrom("test1", "test1::data")).toSearchRequest()),
            Map.of(dataIndexName, Set.of("@timestamp", "age"))
        );

        // FLS applies to failure store
        assertSearchResponseContainsExpectedIndicesAndFields(
            performRequest(user, new Search("test1::failures").toSearchRequest()),
            Map.of(failureIndexName, Set.of("error.type", "error.message"))
        );

        upsertRole(Strings.format("""
            {
                 "cluster": ["all"],
                 "indices": [
                     {
                        "names": ["%s"],
                        "privileges": ["read"],
                        "field_security": {
                            "grant": ["@timestamp", "age"]
                        }
                     },
                     {
                        "names": ["test*"],
                        "privileges": ["read_failure_store"],
                        "field_security": {
                            "grant": ["error.type", "error.message"]
                        }
                     }
                 ]
             }""", "test*"), role);

        // FLS applies to regular data stream
        assertSearchResponseContainsExpectedIndicesAndFields(
            performRequest(user, new Search(randomFrom("test1", "test1::data")).toSearchRequest()),
            Map.of(dataIndexName, Set.of("@timestamp", "age"))
        );

        // FLS applies to failure store
        assertSearchResponseContainsExpectedIndicesAndFields(
            performRequest(user, new Search("test1::failures").toSearchRequest()),
            Map.of(failureIndexName, Set.of("@timestamp", "error.type", "error.message"))
        );

        upsertRole("""
            {
                 "cluster": ["all"],
                 "indices": [
                     {
                        "names": ["test*"],
                        "privileges": ["read"],
                        "field_security": {
                            "grant": ["@timestamp", "age"]
                        }
                     },
                     {
                        "names": ["test*"],
                        "privileges": ["read_failure_store"]
                     }
                 ]
             }""", role);

        // since there is a section without FLS, no FLS applies
        assertSearchResponseContainsExpectedIndicesAndFields(
            performRequest(user, new Search(randomFrom("test1", "test1::data")).toSearchRequest()),
            Map.of(dataIndexName, Set.of("@timestamp", "age", "name", "email"))
        );
        assertSearchResponseContainsExpectedIndicesAndFields(
            performRequest(user, new Search("test1::failures").toSearchRequest()),
            Map.of(
                failureIndexName,
                Set.of(
                    "@timestamp",
                    "document.id",
                    "document.index",
                    "document.source.@timestamp",
                    "document.source.age",
                    "document.source.email",
                    "document.source.name",
                    "error.message",
                    "error.stack_trace",
                    "error.type"
                )
            )
        );

        // check that direct read access to backing indices is working
        upsertRole(Strings.format("""
            {
                 "cluster": ["all"],
                 "indices": [
                     {
                        "names": ["%s"],
                        "privileges": ["read"],
                        "field_security": {
                            "grant": ["@timestamp", "age"]
                        }
                     },
                     {
                        "names": ["%s"],
                        "privileges": ["read"],
                        "field_security": {
                            "grant": ["@timestamp", "document.source.name"]
                        }
                     }
                 ]
             }""", dataIndexName, failureIndexName), role);

        // FLS applies to backing data index
        assertSearchResponseContainsExpectedIndicesAndFields(
            performRequest(user, new Search(randomFrom(dataIndexName, ".ds-*")).toSearchRequest()),
            Map.of(dataIndexName, Set.of("@timestamp", "age"))
        );
        // and backing failure index
        assertSearchResponseContainsExpectedIndicesAndFields(
            performRequest(user, new Search(randomFrom(failureIndexName, ".fs-*")).toSearchRequest()),
            Map.of(failureIndexName, Set.of("@timestamp", "document.source.name"))
        );

        // DLS
        String dataIndexDocId = "1";
        upsertRole("""
            {
                 "cluster": ["all"],
                 "indices": [
                     {
                        "names": ["test*"],
                        "privileges": ["read", "read_failure_store"],
                        "query":{"term":{"name":{"value":"not-jack"}}}
                     }
                 ]
             }""", role);
        // DLS applies and no docs match the query
        expectSearch(user, new Search(randomFrom("test1", "test1::data")));
        expectSearch(user, new Search("test1::failures"));

        upsertRole("""
            {
                 "cluster": ["all"],
                 "indices": [
                     {
                        "names": ["test*"],
                        "privileges": ["read", "read_failure_store"],
                        "query":{"term":{"name":{"value":"jack"}}}
                     }
                 ]
             }""", role);
        // DLS applies and doc matches the query
        expectSearch(user, new Search(randomFrom("test1", "test1::data")), dataIndexDocId);
        expectSearch(user, new Search("test1::failures"));

        upsertRole("""
            {
                 "cluster": ["all"],
                 "indices": [
                     {
                        "names": ["test*"],
                        "privileges": ["read"],
                        "query":{"term":{"name":{"value":"not-jack"}}}
                     },
                     {
                        "names": ["test*"],
                        "privileges": ["read_failure_store"]
                     }
                 ]
             }""", role);
        // DLS does not apply because there is a section without DLS
        expectSearch(user, new Search(randomFrom("test1", "test1::data")), dataIndexDocId);

        // DLS is applicable to backing failure store when granted read directly
        upsertRole(Strings.format("""
            {
                 "cluster": ["all"],
                 "indices": [
                     {
                        "names": ["%s"],
                        "privileges": ["read"],
                        "query":{"term":{"document.source.name":{"value":"jack"}}}
                     }
                 ]
             }""", failureIndexName), role);
        expectSearch(user, new Search(randomFrom(".fs-*", failureIndexName)));

    }

    private static void expectThrows(ThrowingRunnable runnable, int statusCode) {
        var ex = expectThrows(ResponseException.class, runnable);
        assertThat(ex.getResponse().getStatusLine().getStatusCode(), equalTo(statusCode));
    }

    private void expectThrowsBadRequest(ThrowingRunnable runnable, Matcher<String> errorMatcher) {
        ResponseException ex = expectThrows(ResponseException.class, runnable);
        assertThat(ex.getResponse().getStatusLine().getStatusCode(), equalTo(400));
        assertThat(ex.getMessage(), errorMatcher);
    }

    private void expectThrowsUnauthorized(String user, Search search, Matcher<String> errorMatcher) {
        ResponseException ex = expectThrows(ResponseException.class, () -> performRequestMaybeUsingApiKey(user, search.toSearchRequest()));
        assertThat(ex.getResponse().getStatusLine().getStatusCode(), equalTo(403));
        assertThat(ex.getMessage(), errorMatcher);
    }

    private void expectSearchThrows(String user, Search search, int statusCode) {
        expectThrows(() -> performRequestMaybeUsingApiKey(user, search.toSearchRequest()), statusCode);
        expectThrows(() -> performRequestMaybeUsingApiKey(user, search.toAsyncSearchRequest()), statusCode);
    }

    private void expectSearch(String user, Search search, String... docIds) throws Exception {
        expectSearch(performRequestMaybeUsingApiKey(user, search.toSearchRequest()), docIds);
        expectAsyncSearch(performRequestMaybeUsingApiKey(user, search.toAsyncSearchRequest()), docIds);
    }

    private void expectSearchWithApiKey(String apiKey, Search search, String... docIds) throws Exception {
        expectSearch(performRequestWithApiKey(apiKey, search.toSearchRequest()), docIds);
        expectAsyncSearch(performRequestWithApiKey(apiKey, search.toAsyncSearchRequest()), docIds);
    }

    private void expectThrowsWithApiKey(String apiKey, Search search, int statusCode) {
        expectThrows(() -> performRequestWithApiKey(apiKey, search.toSearchRequest()), statusCode);
        expectThrows(() -> performRequestWithApiKey(apiKey, search.toAsyncSearchRequest()), statusCode);
    }

    @SuppressWarnings("unchecked")
    private static void expectAsyncSearch(Response response, String... docIds) throws IOException {
        assertOK(response);
        ObjectPath resp = ObjectPath.createFromResponse(response);
        Boolean isRunning = resp.evaluate("is_running");
        Boolean isPartial = resp.evaluate("is_partial");
        assertThat(isRunning, is(false));
        assertThat(isPartial, is(false));

        List<Object> hits = resp.evaluate("response.hits.hits");
        List<String> actual = hits.stream().map(h -> (String) ((Map<String, Object>) h).get("_id")).toList();

        assertThat(actual, containsInAnyOrder(docIds));
    }

    private static void expectSearch(Response response, String... docIds) throws IOException {
        assertOK(response);
        final SearchResponse searchResponse = SearchResponseUtils.parseSearchResponse(responseAsParser(response));
        try {
            SearchHit[] hits = searchResponse.getHits().getHits();
            if (docIds != null) {
                assertThat(Arrays.toString(hits), hits.length, equalTo(docIds.length));
                List<String> actualDocIds = Arrays.stream(hits).map(SearchHit::getId).toList();
                assertThat(actualDocIds, containsInAnyOrder(docIds));
            } else {
                assertThat(hits.length, equalTo(0));
            }
        } finally {
            searchResponse.decRef();
        }
    }

    private record Search(String searchTarget, String pathParamString) {
        Search(String searchTarget) {
            this(searchTarget, "");
        }

        Request toSearchRequest() {
            return new Request("POST", Strings.format("/%s/_search%s", searchTarget, pathParamString));
        }

        Request toAsyncSearchRequest() {
            var pathParam = pathParamString.isEmpty()
                ? "?wait_for_completion_timeout=" + ASYNC_SEARCH_TIMEOUT
                : pathParamString + "&wait_for_completion_timeout=" + ASYNC_SEARCH_TIMEOUT;
            return new Request("POST", Strings.format("/%s/_async_search%s", searchTarget, pathParam));
        }

        Request toEsqlRequest() throws IOException {
            String command = "from " + searchTarget + " METADATA _id | KEEP _id";
            if (command.toLowerCase(Locale.ROOT).contains("limit") == false) {
                // add a (high) limit to avoid warnings on default limit
                command += " | limit 10000000";
            }
            XContentBuilder json = JsonXContent.contentBuilder();
            json.startObject();
            json.field("query", command);
            addRandomPragmas(json);
            json.endObject();
            var request = new Request("POST", "_query");
            request.setJsonEntity(Strings.toString(json));
            return request;
        }
    }

    private List<String> setupDataStream() throws IOException {
        createTemplates();
        return randomBoolean() ? populateDataStreamWithBulkRequest() : populateDataStreamWithDocRequests();
    }

    @SuppressWarnings("unchecked")
    private List<String> setupOtherDataStream() throws IOException {
        createOtherTemplates();

        var bulkRequest = new Request("POST", "/_bulk?refresh=true");
        bulkRequest.setJsonEntity("""
            { "create" : { "_index" : "other1", "_id" : "3" } }
            { "@timestamp": 3, "age" : 1, "name" : "jane", "email" : "jane@example.com" }
            { "create" : { "_index" : "other1", "_id" : "4" } }
            { "@timestamp": 4, "age" : "this should be an int", "name" : "jane", "email" : "jane@example.com" }
            """);
        Response response = performRequest(WRITE_ACCESS, bulkRequest);
        assertOK(response);
        // we need this dance because the ID for the failed document is random, **not** 4
        Map<String, Object> stringObjectMap = responseAsMap(response);
        List<Object> items = (List<Object>) stringObjectMap.get("items");
        List<String> ids = new ArrayList<>();
        for (Object item : items) {
            Map<String, Object> itemMap = (Map<String, Object>) item;
            Map<String, Object> create = (Map<String, Object>) itemMap.get("create");
            assertThat(create.get("status"), equalTo(201));
            ids.add((String) create.get("_id"));
        }
        return ids;
    }

    private void createTemplates() throws IOException {
        var componentTemplateRequest = new Request("PUT", "/_component_template/component1");
        componentTemplateRequest.setJsonEntity("""
            {
                "template": {
                    "mappings": {
                        "properties": {
                            "@timestamp": {
                                "type": "date"
                            },
                            "age": {
                                "type": "integer"
                            },
                            "email": {
                                "type": "keyword"
                            },
                            "name": {
                                "type": "text"
                            }
                        }
                    },
                    "data_stream_options": {
                      "failure_store": {
                        "enabled": true
                      }
                    }
                }
            }
            """);
        assertOK(adminClient().performRequest(componentTemplateRequest));

        var indexTemplateRequest = new Request("PUT", "/_index_template/template1");
        indexTemplateRequest.setJsonEntity("""
            {
                "index_patterns": ["test*"],
                "data_stream": {},
                "priority": 500,
                "composed_of": ["component1"]
            }
            """);
        assertOK(adminClient().performRequest(indexTemplateRequest));
    }

    private void createOtherTemplates() throws IOException {
        var componentTemplateRequest = new Request("PUT", "/_component_template/component2");
        componentTemplateRequest.setJsonEntity("""
            {
                "template": {
                    "mappings": {
                        "properties": {
                            "@timestamp": {
                                "type": "date"
                            },
                            "age": {
                                "type": "integer"
                            },
                            "email": {
                                "type": "keyword"
                            },
                            "name": {
                                "type": "text"
                            }
                        }
                    },
                    "data_stream_options": {
                      "failure_store": {
                        "enabled": true
                      }
                    }
                }
            }
            """);
        assertOK(adminClient().performRequest(componentTemplateRequest));

        var indexTemplateRequest = new Request("PUT", "/_index_template/template2");
        indexTemplateRequest.setJsonEntity("""
            {
                "index_patterns": ["other*"],
                "data_stream": {},
                "priority": 500,
                "composed_of": ["component1"]
            }
            """);
        assertOK(adminClient().performRequest(indexTemplateRequest));
    }

    private List<String> populateDataStreamWithDocRequests() throws IOException {
        List<String> ids = new ArrayList<>();

        var dataStreamName = "test1";
        var docRequest = new Request("PUT", "/" + dataStreamName + "/_doc/1?refresh=true&op_type=create");
        docRequest.setJsonEntity("""
            {
               "@timestamp": 1,
               "age" : 1,
               "name" : "jack",
               "email" : "jack@example.com"
            }
            """);
        Response response = performRequest(WRITE_ACCESS, docRequest);
        assertOK(response);
        Map<String, Object> responseAsMap = responseAsMap(response);
        ids.add((String) responseAsMap.get("_id"));

        docRequest = new Request("PUT", "/" + dataStreamName + "/_doc/2?refresh=true&op_type=create");
        docRequest.setJsonEntity("""
            {
               "@timestamp": 2,
               "age" : "this should be an int",
               "name" : "jack",
               "email" : "jack@example.com"
            }
            """);
        response = performRequest(WRITE_ACCESS, docRequest);
        assertOK(response);
        responseAsMap = responseAsMap(response);
        ids.add((String) responseAsMap.get("_id"));

        return ids;
    }

    @SuppressWarnings("unchecked")
    private List<String> populateDataStreamWithBulkRequest() throws IOException {
        var bulkRequest = new Request("POST", "/_bulk?refresh=true");
        bulkRequest.setJsonEntity("""
            { "create" : { "_index" : "test1", "_id" : "1" } }
            { "@timestamp": 1, "age" : 1, "name" : "jack", "email" : "jack@example.com" }
            { "create" : { "_index" : "test1", "_id" : "2" } }
            { "@timestamp": 2, "age" : "this should be an int", "name" : "jack", "email" : "jack@example.com" }
            """);
        Response response = performRequest(WRITE_ACCESS, bulkRequest);
        assertOK(response);
        // we need this dance because the ID for the failed document is random, **not** 2
        Map<String, Object> stringObjectMap = responseAsMap(response);
        List<Object> items = (List<Object>) stringObjectMap.get("items");
        List<String> ids = new ArrayList<>();
        for (Object item : items) {
            Map<String, Object> itemMap = (Map<String, Object>) item;
            Map<String, Object> create = (Map<String, Object>) itemMap.get("create");
            assertThat(create.get("status"), equalTo(201));
            ids.add((String) create.get("_id"));
        }
        return ids;
    }

    private void deleteDataStream(String user, String dataStreamName) throws IOException {
        assertOK(performRequest(user, new Request("DELETE", "/_data_stream/" + dataStreamName)));
    }

    private void deleteIndex(String user, String indexName) throws IOException {
        assertOK(performRequest(user, new Request("DELETE", "/" + indexName)));
    }

    private Response performRequest(String user, Request request) throws IOException {
        request.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", basicAuthHeaderValue(user, PASSWORD)).build());
        return client().performRequest(request);
    }

    private Response performRequestWithRunAs(String user, Request request) throws IOException {
        request.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader("es-security-runas-user", user).build());
        return adminClient().performRequest(request);
    }

    private Response performRequestMaybeUsingApiKey(String user, Request request) throws IOException {
        if (randomBoolean() && apiKeys.containsKey(user)) {
            return performRequestWithApiKey(apiKeys.get(user), request);
        } else {
            return performRequest(user, request);
        }
    }

    private static Response performRequestWithApiKey(String apiKey, Request request) throws IOException {
        request.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", "ApiKey " + apiKey).build());
        return client().performRequest(request);
    }

    private static void expectUserPrivilegesResponse(String userPrivilegesResponse) throws IOException {
        Request request = new Request("GET", "/_security/user/_privileges");
        request.setOptions(request.getOptions().toBuilder().addHeader("Authorization", basicAuthHeaderValue("user", PASSWORD)));
        Response response = client().performRequest(request);
        assertOK(response);
        assertThat(responseAsMap(response), equalTo(mapFromJson(userPrivilegesResponse)));
    }

    private static Map<String, Object> mapFromJson(String json) {
        return XContentHelper.convertToMap(JsonXContent.jsonXContent, json, false);
    }

    protected TestSecurityClient getSecurityClient() {
        if (securityClient == null) {
            securityClient = new TestSecurityClient(adminClient());
        }
        return securityClient;
    }

    protected void createUser(String username, SecureString password, String... roles) throws IOException {
        getSecurityClient().putUser(new User(username, roles), password);
    }

    protected String createAndStoreApiKey(String username, @Nullable String roleDescriptors) throws IOException {
        assertThat("API key already registered for user: " + username, apiKeys.containsKey(username), is(false));
        apiKeys.put(username, createApiKey(username, roleDescriptors));
        return apiKeys.get(username);
    }

    protected String createOrUpdateApiKey(String username, @Nullable String roleDescriptors) throws IOException {
        apiKeys.put(username, createApiKey(username, roleDescriptors));
        return apiKeys.get(username);
    }

    private String createApiKey(String username, String roleDescriptors) throws IOException {
        var request = new Request("POST", "/_security/api_key");
        if (roleDescriptors == null) {
            request.setJsonEntity("""
                {
                    "name": "test-api-key"
                }
                """);
        } else {
            request.setJsonEntity(Strings.format("""
                {
                    "name": "test-api-key",
                    "role_descriptors": %s
                }
                """, roleDescriptors));
        }
        Response response = performRequest(username, request);
        assertOK(response);
        Map<String, Object> responseAsMap = responseAsMap(response);
        return (String) responseAsMap.get("encoded");
    }

    protected Response upsertRole(String roleDescriptor, String roleName) throws IOException {
        return upsertRole(roleDescriptor, roleName, randomBoolean());
    }

    protected Response upsertRole(String roleDescriptor, String roleName, boolean bulk) throws IOException {
        Request createRoleRequest = roleRequest(roleDescriptor, roleName, bulk);
        Response createRoleResponse = adminClient().performRequest(createRoleRequest);
        assertOK(createRoleResponse);
        if (bulk) {
            Map<String, Object> flattenedResponse = Maps.flatten(responseAsMap(createRoleResponse), true, true);
            if (flattenedResponse.containsKey("errors.count") && (int) flattenedResponse.get("errors.count") > 0) {
                throw new AssertionError(
                    "Failed to create role [" + roleName + "], reason: " + flattenedResponse.get("errors.details." + roleName + ".reason")
                );
            }
        }
        return createRoleResponse;
    }

    protected Request roleRequest(String roleDescriptor, String roleName, boolean bulk) {
        Request createRoleRequest;
        if (bulk) {
            createRoleRequest = new Request(HttpPost.METHOD_NAME, "/_security/role");
            createRoleRequest.setJsonEntity(org.elasticsearch.core.Strings.format("""
                {"roles": {"%s": %s}}
                """, roleName, roleDescriptor));
        } else {
            createRoleRequest = new Request(randomFrom(HttpPut.METHOD_NAME, HttpPost.METHOD_NAME), "/_security/role/" + roleName);
            createRoleRequest.setJsonEntity(roleDescriptor);
        }
        return createRoleRequest;
    }

    protected void assertSearchResponseContainsExpectedIndicesAndFields(
        Response searchResponse,
        Map<String, Set<String>> expectedIndicesAndFields
    ) {
        try {
            assertOK(searchResponse);
            var response = SearchResponseUtils.responseAsSearchResponse(searchResponse);
            try {
                final var searchResult = Arrays.stream(response.getHits().getHits())
                    .collect(Collectors.toMap(SearchHit::getIndex, SearchHit::getSourceAsMap));

                assertThat(searchResult.keySet(), equalTo(expectedIndicesAndFields.keySet()));
                for (String index : expectedIndicesAndFields.keySet()) {
                    Set<String> expectedFields = expectedIndicesAndFields.get(index);
                    assertThat(Maps.flatten(searchResult.get(index), false, true).keySet(), equalTo(expectedFields));
                }
            } finally {
                response.decRef();
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void deleteScroll(String scrollId) throws IOException {
        Request deleteScroll = new Request("DELETE", "/_search/scroll");
        deleteScroll.setJsonEntity(Strings.format("""
            {
                "scroll_id": "%s"
            }
            """, scrollId));
        Response deleteScrollResponse = performRequest("user", deleteScroll);
        assertOK(deleteScrollResponse);
    }

    private String performScrollSearchRequestAndAssertDocs(String indexExpression, String docId) throws IOException {
        Response scrollResponse = performRequest("user", new Request("POST", Strings.format("/%s/_search?scroll=1m", indexExpression)));
        assertOK(scrollResponse);

        final SearchResponse searchResponse = SearchResponseUtils.parseSearchResponse(responseAsParser(scrollResponse));
        final String scrollId = searchResponse.getScrollId();
        assertThat(scrollId, notNullValue());
        try {
            assertSearchContainsDocs(searchResponse, docId);
        } finally {
            searchResponse.decRef();
        }
        return scrollId;
    }

    private SearchResponse performScrollSearchRequest(String user, String scrollId) throws IOException {
        Request searchRequestWithScrollId = new Request("POST", "/_search/scroll");
        searchRequestWithScrollId.setJsonEntity(Strings.format("""
            {
                "scroll": "1m",
                "scroll_id": "%s"
            }
            """, scrollId));
        Response response = performRequest(user, searchRequestWithScrollId);
        assertOK(response);
        return SearchResponseUtils.parseSearchResponse(responseAsParser(response));
    }

    private static void assertSearchContainsDocs(SearchResponse searchResponse, String... docIds) {
        SearchHit[] hits = searchResponse.getHits().getHits();
        assertThat(hits.length, equalTo(docIds.length));
        List<String> actualDocIds = Arrays.stream(hits).map(SearchHit::getId).toList();
        assertThat(actualDocIds, containsInAnyOrder(docIds));
    }

    private static void assertSearchHasNoHits(SearchResponse searchResponse) {
        try {
            SearchHit[] hits = searchResponse.getHits().getHits();
            assertThat(hits.length, equalTo(0));
        } finally {
            searchResponse.decRef();
        }
    }

    static void addRandomPragmas(XContentBuilder builder) throws IOException {
        if (Build.current().isSnapshot()) {
            Settings pragmas = randomPragmas();
            if (pragmas != Settings.EMPTY) {
                builder.startObject("pragma");
                builder.value(pragmas);
                builder.endObject();
            }
        }
    }

    static Settings randomPragmas() {
        Settings.Builder settings = Settings.builder();
        if (randomBoolean()) {
            settings.put("page_size", between(1, 5));
        }
        if (randomBoolean()) {
            settings.put("exchange_buffer_size", between(1, 2));
        }
        if (randomBoolean()) {
            settings.put("data_partitioning", randomFrom("shard", "segment", "doc"));
        }
        if (randomBoolean()) {
            settings.put("enrich_max_workers", between(1, 5));
        }
        if (randomBoolean()) {
            settings.put("node_level_reduction", randomBoolean());
        }
        return settings.build();
    }

    @SuppressWarnings("unchecked")
    private Tuple<List<String>, List<String>> getDataAndFailureIndices(String dataStreamName) throws IOException {
        Request dataStream = new Request("GET", "/_data_stream/" + dataStreamName);
        Response response = adminClient().performRequest(dataStream);
        Map<String, Object> dataStreams = entityAsMap(response);
        assertEquals(Collections.singletonList("test1"), XContentMapValues.extractValue("data_streams.name", dataStreams));
        List<String> dataIndexNames = (List<String>) XContentMapValues.extractValue("data_streams.indices.index_name", dataStreams);
        List<String> failureIndexNames = (List<String>) XContentMapValues.extractValue(
            "data_streams.failure_store.indices.index_name",
            dataStreams
        );
        return new Tuple<>(dataIndexNames, failureIndexNames);
    }

    private Tuple<String, String> getSingleDataAndFailureIndices(String dataStreamName) throws IOException {
        Tuple<List<String>, List<String>> indices = getDataAndFailureIndices(dataStreamName);
        assertThat(indices.v1().size(), equalTo(1));
        assertThat(indices.v2().size(), equalTo(1));
        return new Tuple<>(indices.v1().get(0), indices.v2().get(0));
    }

    private void expectHasPrivileges(String user, String requestBody, String expectedResponse) throws IOException {
        Request req = new Request("POST", "/_security/user/_has_privileges");
        req.setJsonEntity(requestBody);
        Response response = randomBoolean() ? performRequestMaybeUsingApiKey(user, req) : performRequestWithRunAs(user, req);
        assertThat(responseAsMap(response), equalTo(mapFromJson(expectedResponse)));
    }

    private void expectHasPrivilegesWithApiKey(String apiKey, String requestBody, String expectedResponse) throws IOException {
        Request req = new Request("POST", "/_security/user/_has_privileges");
        req.setJsonEntity(requestBody);
        Response response = performRequestWithApiKey(apiKey, req);
        assertThat(responseAsMap(response), equalTo(mapFromJson(expectedResponse)));
    }

    private static void expectThrowsSelectorsNotAllowed(ThrowingRunnable runnable) {
        ResponseException exception = expectThrows(ResponseException.class, runnable);
        assertThat(exception.getResponse().getStatusLine().getStatusCode(), equalTo(400));
        assertThat(exception.getMessage(), containsString("selectors [::] are not allowed in the index name expression"));
    }
}

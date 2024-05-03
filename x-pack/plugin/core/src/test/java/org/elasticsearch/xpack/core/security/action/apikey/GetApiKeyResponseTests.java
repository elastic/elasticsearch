/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.apikey;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.core.security.action.apikey.CrossClusterApiKeyRoleDescriptorBuilder.CCR_INDICES_PRIVILEGE_NAMES;
import static org.elasticsearch.xpack.core.security.action.apikey.CrossClusterApiKeyRoleDescriptorBuilder.CCS_AND_CCR_CLUSTER_PRIVILEGE_NAMES;
import static org.elasticsearch.xpack.core.security.action.apikey.CrossClusterApiKeyRoleDescriptorBuilder.CCS_INDICES_PRIVILEGE_NAMES;
import static org.elasticsearch.xpack.core.security.action.apikey.CrossClusterApiKeyRoleDescriptorBuilder.ROLE_DESCRIPTOR_NAME;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class GetApiKeyResponseTests extends ESTestCase {

    public void testToXContent() throws IOException {
        final List<RoleDescriptor> roleDescriptors = List.of(
            new RoleDescriptor(
                "rd_42",
                new String[] { "monitor" },
                new RoleDescriptor.IndicesPrivileges[] {
                    RoleDescriptor.IndicesPrivileges.builder().indices("index").privileges("read").build() },
                new String[] { "foo" }
            )
        );
        final List<RoleDescriptor> limitedByRoleDescriptors = List.of(
            new RoleDescriptor(
                "rd_0",
                new String[] { "all" },
                new RoleDescriptor.IndicesPrivileges[] {
                    RoleDescriptor.IndicesPrivileges.builder().indices("index").privileges("all").build() },
                new String[] { "*" }
            )
        );

        ApiKey apiKeyInfo1 = createApiKeyInfo(
            "name1",
            "id-1",
            ApiKey.Type.REST,
            Instant.ofEpochMilli(100000L),
            Instant.ofEpochMilli(10000000L),
            false,
            null,
            "user-a",
            "realm-x",
            null,
            null,
            null,
            List.of() // empty limited-by role descriptor to simulate derived keys
        );
        ApiKey apiKeyInfo2 = createApiKeyInfo(
            "name2",
            "id-2",
            ApiKey.Type.REST,
            Instant.ofEpochMilli(100000L),
            Instant.ofEpochMilli(10000000L),
            true,
            Instant.ofEpochMilli(100000000L),
            "user-b",
            "realm-y",
            "realm-type-y",
            Map.of(),
            List.of(),
            limitedByRoleDescriptors
        );
        ApiKey apiKeyInfo3 = createApiKeyInfo(
            null,
            "id-3",
            ApiKey.Type.REST,
            Instant.ofEpochMilli(100000L),
            null,
            true,
            Instant.ofEpochMilli(100000000L),
            "user-c",
            "realm-z",
            "realm-type-z",
            Map.of("foo", "bar"),
            roleDescriptors,
            limitedByRoleDescriptors
        );
        final List<RoleDescriptor> crossClusterAccessRoleDescriptors = List.of(
            new RoleDescriptor(
                ROLE_DESCRIPTOR_NAME,
                CCS_AND_CCR_CLUSTER_PRIVILEGE_NAMES,
                new RoleDescriptor.IndicesPrivileges[] {
                    RoleDescriptor.IndicesPrivileges.builder().indices("logs").privileges(CCS_INDICES_PRIVILEGE_NAMES).build(),
                    RoleDescriptor.IndicesPrivileges.builder().indices("archive").privileges(CCR_INDICES_PRIVILEGE_NAMES).build(), },
                null
            )
        );
        ApiKey apiKeyInfo4 = createApiKeyInfo(
            "name4",
            "id-4",
            ApiKey.Type.CROSS_CLUSTER,
            Instant.ofEpochMilli(100000L),
            null,
            true,
            Instant.ofEpochMilli(100000000L),
            "user-c",
            "realm-z",
            "realm-type-z",
            Map.of("foo", "bar"),
            crossClusterAccessRoleDescriptors,
            null
        );
        String profileUid2 = "profileUid2";
        String profileUid4 = "profileUid4";
        List<String> profileUids = new ArrayList<>(4);
        profileUids.add(null);
        profileUids.add(profileUid2);
        profileUids.add(null);
        profileUids.add(profileUid4);
        GetApiKeyResponse response = new GetApiKeyResponse(Arrays.asList(apiKeyInfo1, apiKeyInfo2, apiKeyInfo3, apiKeyInfo4), profileUids);
        XContentBuilder builder = XContentFactory.jsonBuilder();
        response.toXContent(builder, ToXContent.EMPTY_PARAMS);
        assertThat(Strings.toString(builder), equalTo(XContentHelper.stripWhitespace(Strings.format("""
            {
              "api_keys": [
                {
                  "id": "id-1",
                  "name": "name1",
                  %s
                  "creation": 100000,
                  "expiration": 10000000,
                  "invalidated": false,
                  "username": "user-a",
                  "realm": "realm-x",
                  "metadata": {},
                  "limited_by": [
                    { }
                  ]
                },
                {
                  "id": "id-2",
                  "name": "name2",
                  %s
                  "creation": 100000,
                  "expiration": 10000000,
                  "invalidated": true,
                  "invalidation": 100000000,
                  "username": "user-b",
                  "realm": "realm-y",
                  "realm_type": "realm-type-y",
                  "metadata": {},
                  "role_descriptors": {},
                  "limited_by": [
                    {
                      "rd_0": {
                        "cluster": [
                          "all"
                        ],
                        "indices": [
                          {
                            "names": [
                              "index"
                            ],
                            "privileges": [
                              "all"
                            ],
                            "allow_restricted_indices": false
                          }
                        ],
                        "applications": [],
                        "run_as": [
                          "*"
                        ],
                        "metadata": {},
                        "transient_metadata": {
                          "enabled": true
                        }
                      }
                    }
                  ],
                  "profile_uid": "profileUid2"
                },
                {
                  "id": "id-3",
                  "name": null,
                  %s
                  "creation": 100000,
                  "invalidated": true,
                  "invalidation": 100000000,
                  "username": "user-c",
                  "realm": "realm-z",
                  "realm_type": "realm-type-z",
                  "metadata": {
                    "foo": "bar"
                  },
                  "role_descriptors": {
                    "rd_42": {
                      "cluster": [
                        "monitor"
                      ],
                      "indices": [
                        {
                          "names": [
                            "index"
                          ],
                          "privileges": [
                            "read"
                          ],
                          "allow_restricted_indices": false
                        }
                      ],
                      "applications": [],
                      "run_as": [
                        "foo"
                      ],
                      "metadata": {},
                      "transient_metadata": {
                        "enabled": true
                      }
                    }
                  },
                  "limited_by": [
                    {
                      "rd_0": {
                        "cluster": [
                          "all"
                        ],
                        "indices": [
                          {
                            "names": [
                              "index"
                            ],
                            "privileges": [
                              "all"
                            ],
                            "allow_restricted_indices": false
                          }
                        ],
                        "applications": [],
                        "run_as": [
                          "*"
                        ],
                        "metadata": {},
                        "transient_metadata": {
                          "enabled": true
                        }
                      }
                    }
                  ]
                },
                {
                  "id": "id-4",
                  "name": "name4",
                  %s
                  "creation": 100000,
                  "invalidated": true,
                  "invalidation": 100000000,
                  "username": "user-c",
                  "realm": "realm-z",
                  "realm_type": "realm-type-z",
                  "metadata": {
                    "foo": "bar"
                  },
                  "role_descriptors": {
                    "cross_cluster": {
                      "cluster": [
                        "cross_cluster_search", "monitor_enrich", "cross_cluster_replication"
                      ],
                      "indices": [
                        {
                          "names": [
                            "logs"
                          ],
                          "privileges": [
                            "read", "read_cross_cluster", "view_index_metadata"
                          ],
                          "allow_restricted_indices": false
                        },
                        {
                          "names": [
                            "archive"
                          ],
                          "privileges": [
                            "cross_cluster_replication", "cross_cluster_replication_internal"
                          ],
                          "allow_restricted_indices": false
                        }
                      ],
                      "applications": [],
                      "run_as": [],
                      "metadata": {},
                      "transient_metadata": {
                        "enabled": true
                      }
                    }
                  },
                  "access": {
                    "search": [
                      {
                        "names": [
                          "logs"
                        ],
                        "allow_restricted_indices": false
                      }
                    ],
                    "replication": [
                      {
                        "names": [
                          "archive"
                        ],
                        "allow_restricted_indices": false
                      }
                    ]
                  },
                  "profile_uid": "profileUid4"
                }
              ]
            }""", getType("rest"), getType("rest"), getType("rest"), getType("cross_cluster")))));
    }

    public void testMismatchApiKeyInfoAndProfileData() {
        List<ApiKey> apiKeys = randomList(
            0,
            3,
            () -> new ApiKey(
                randomAlphaOfLength(4),
                randomAlphaOfLength(4),
                randomFrom(ApiKey.Type.values()),
                Instant.now(),
                Instant.now(),
                randomBoolean(),
                null,
                randomAlphaOfLength(4),
                randomAlphaOfLength(4),
                null,
                null,
                null,
                null
            )
        );
        List<String> profileUids = randomList(0, 5, () -> randomFrom(randomAlphaOfLength(4), null));
        if (apiKeys.size() != profileUids.size()) {
            IllegalStateException ise = expectThrows(IllegalStateException.class, () -> new GetApiKeyResponse(apiKeys, profileUids));
            assertThat(ise.getMessage(), containsString("Each api key info must be associated to a (nullable) owner profile uid"));
        }
    }

    private ApiKey createApiKeyInfo(
        String name,
        String id,
        ApiKey.Type type,
        Instant creation,
        Instant expiration,
        boolean invalidated,
        Instant invalidation,
        String username,
        String realm,
        String realmType,
        Map<String, Object> metadata,
        List<RoleDescriptor> roleDescriptors,
        List<RoleDescriptor> limitedByRoleDescriptors
    ) {
        return new ApiKey(
            name,
            id,
            type,
            creation,
            expiration,
            invalidated,
            invalidation,
            username,
            realm,
            realmType,
            metadata,
            roleDescriptors,
            limitedByRoleDescriptors
        );
    }

    private String getType(String type) {
        return "\"type\": \"" + type + "\",";
    }
}

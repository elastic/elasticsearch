/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.apikey;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.TcpTransport;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.privilege.ConfigurableClusterPrivilege;
import org.elasticsearch.xpack.core.security.authz.privilege.ConfigurableClusterPrivileges;

import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.core.security.authz.RoleDescriptorTests.randomUniquelyNamedRoleDescriptors;
import static org.hamcrest.Matchers.equalTo;

public class GetApiKeyResponseTests extends ESTestCase {

    public void testSerialization() throws IOException {
        boolean withApiKeyName = randomBoolean();
        boolean withExpiration = randomBoolean();
        final ApiKey.Type type = randomFrom(ApiKey.Type.values());
        ApiKey apiKeyInfo = createApiKeyInfo(
            (withApiKeyName) ? randomAlphaOfLength(4) : null,
            randomAlphaOfLength(5),
            type,
            Instant.now(),
            (withExpiration) ? Instant.now() : null,
            false,
            randomAlphaOfLength(4),
            randomAlphaOfLength(5),
            randomBoolean() ? null : Map.of(randomAlphaOfLengthBetween(3, 8), randomAlphaOfLengthBetween(3, 8)),
            randomBoolean() ? null : randomUniquelyNamedRoleDescriptors(0, 3),
            type == ApiKey.Type.CROSS_CLUSTER ? null : randomUniquelyNamedRoleDescriptors(1, 3)
        );
        GetApiKeyResponse response = new GetApiKeyResponse(Collections.singletonList(apiKeyInfo));

        final NamedWriteableRegistry namedWriteableRegistry = new NamedWriteableRegistry(
            List.of(
                new NamedWriteableRegistry.Entry(
                    ConfigurableClusterPrivilege.class,
                    ConfigurableClusterPrivileges.ManageApplicationPrivileges.WRITEABLE_NAME,
                    ConfigurableClusterPrivileges.ManageApplicationPrivileges::createFrom
                ),
                new NamedWriteableRegistry.Entry(
                    ConfigurableClusterPrivilege.class,
                    ConfigurableClusterPrivileges.WriteProfileDataPrivileges.WRITEABLE_NAME,
                    ConfigurableClusterPrivileges.WriteProfileDataPrivileges::createFrom
                )
            )
        );

        try (BytesStreamOutput output = new BytesStreamOutput()) {
            response.writeTo(output);
            try (StreamInput input = new NamedWriteableAwareStreamInput(output.bytes().streamInput(), namedWriteableRegistry)) {
                GetApiKeyResponse serialized = new GetApiKeyResponse(input);
                assertThat(serialized.getApiKeyInfos(), equalTo(response.getApiKeyInfos()));
            }
        }
    }

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
            "user-a",
            "realm-x",
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
            "user-b",
            "realm-y",
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
            "user-c",
            "realm-z",
            Map.of("foo", "bar"),
            roleDescriptors,
            limitedByRoleDescriptors
        );
        ApiKey apiKeyInfo4 = createApiKeyInfo(
            "name4",
            "id-4",
            ApiKey.Type.CROSS_CLUSTER,
            Instant.ofEpochMilli(100000L),
            null,
            true,
            "user-c",
            "realm-z",
            Map.of("foo", "bar"),
            roleDescriptors,
            null
        );
        GetApiKeyResponse response = new GetApiKeyResponse(Arrays.asList(apiKeyInfo1, apiKeyInfo2, apiKeyInfo3, apiKeyInfo4));
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
                  "username": "user-b",
                  "realm": "realm-y",
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
                  ]
                },
                {
                  "id": "id-3",
                  "name": null,
                  %s
                  "creation": 100000,
                  "invalidated": true,
                  "username": "user-c",
                  "realm": "realm-z",
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
                  "username": "user-c",
                  "realm": "realm-z",
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
                  }
                }
              ]
            }""", getType("rest"), getType("rest"), getType("rest"), getType("cross_cluster")))));
    }

    private ApiKey createApiKeyInfo(
        String name,
        String id,
        ApiKey.Type type,
        Instant creation,
        Instant expiration,
        boolean invalidated,
        String username,
        String realm,
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
            username,
            realm,
            metadata,
            roleDescriptors,
            limitedByRoleDescriptors
        );
    }

    private String getType(String type) {
        return TcpTransport.isUntrustedRemoteClusterEnabled() ? "\"type\": \"" + type + "\"," : "";
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.security;

import org.elasticsearch.client.security.user.privileges.IndicesPrivileges;
import org.elasticsearch.client.security.user.privileges.Role;
import org.elasticsearch.client.security.user.privileges.Role.ClusterPrivilegeName;
import org.elasticsearch.client.security.user.privileges.Role.IndexPrivilegeName;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.equalTo;

public class CreateApiKeyRequestTests extends ESTestCase {

    public void test() throws IOException {
        List<Role> roles = new ArrayList<>();
        roles.add(
            Role.builder()
                .name("r1")
                .clusterPrivileges(ClusterPrivilegeName.ALL)
                .indicesPrivileges(IndicesPrivileges.builder().indices("ind-x").privileges(IndexPrivilegeName.ALL).build())
                .build()
        );
        roles.add(
            Role.builder()
                .name("r2")
                .clusterPrivileges(ClusterPrivilegeName.ALL)
                .indicesPrivileges(IndicesPrivileges.builder().indices("ind-y").privileges(IndexPrivilegeName.ALL).build())
                .build()
        );

        final Map<String, Object> apiKeyMetadata = randomMetadata();
        CreateApiKeyRequest createApiKeyRequest = new CreateApiKeyRequest("api-key", roles, null, null, apiKeyMetadata);

        Map<String, Object> expected = new HashMap<>(
            org.elasticsearch.core.Map.of(
                "name",
                "api-key",
                "role_descriptors",
                org.elasticsearch.core.Map.of(
                    "r1",
                    org.elasticsearch.core.Map.of(
                        "applications",
                        org.elasticsearch.core.List.of(),
                        "cluster",
                        org.elasticsearch.core.List.of("all"),
                        "indices",
                        org.elasticsearch.core.List.of(
                            org.elasticsearch.core.Map.of(
                                "names",
                                org.elasticsearch.core.List.of("ind-x"),
                                "privileges",
                                org.elasticsearch.core.List.of("all"),
                                "allow_restricted_indices",
                                false
                            )
                        ),
                        "metadata",
                        org.elasticsearch.core.Map.of(),
                        "run_as",
                        org.elasticsearch.core.List.of()
                    ),
                    "r2",
                    org.elasticsearch.core.Map.of(
                        "applications",
                        org.elasticsearch.core.List.of(),
                        "cluster",
                        org.elasticsearch.core.List.of("all"),
                        "indices",
                        org.elasticsearch.core.List.of(
                            org.elasticsearch.core.Map.of(
                                "names",
                                org.elasticsearch.core.List.of("ind-y"),
                                "privileges",
                                org.elasticsearch.core.List.of("all"),
                                "allow_restricted_indices",
                                false
                            )
                        ),
                        "metadata",
                        org.elasticsearch.core.Map.of(),
                        "run_as",
                        org.elasticsearch.core.List.of()
                    )
                )
            )
        );
        if (apiKeyMetadata != null) {
            expected.put("metadata", apiKeyMetadata);
        }

        assertThat(
            XContentHelper.convertToMap(XContentHelper.toXContent(createApiKeyRequest, XContentType.JSON, false), false, XContentType.JSON)
                .v2(),
            equalTo(expected)
        );
    }

    public void testEqualsHashCode() {
        final String name = randomAlphaOfLength(5);
        List<Role> roles = Collections.singletonList(
            Role.builder()
                .name("r1")
                .clusterPrivileges(ClusterPrivilegeName.ALL)
                .indicesPrivileges(IndicesPrivileges.builder().indices("ind-x").privileges(IndexPrivilegeName.ALL).build())
                .build()
        );
        final TimeValue expiration = null;
        final RefreshPolicy refreshPolicy = randomFrom(RefreshPolicy.values());

        CreateApiKeyRequest createApiKeyRequest = new CreateApiKeyRequest(name, roles, expiration, refreshPolicy, randomMetadata());

        EqualsHashCodeTestUtils.checkEqualsAndHashCode(createApiKeyRequest, (original) -> {
            return new CreateApiKeyRequest(
                original.getName(),
                original.getRoles(),
                original.getExpiration(),
                original.getRefreshPolicy(),
                original.getMetadata()
            );
        });
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(createApiKeyRequest, (original) -> {
            return new CreateApiKeyRequest(
                original.getName(),
                original.getRoles(),
                original.getExpiration(),
                original.getRefreshPolicy(),
                original.getMetadata()
            );
        }, CreateApiKeyRequestTests::mutateTestItem);
    }

    private static CreateApiKeyRequest mutateTestItem(CreateApiKeyRequest original) {
        switch (randomIntBetween(0, 4)) {
            case 0:
                return new CreateApiKeyRequest(
                    randomAlphaOfLength(5),
                    original.getRoles(),
                    original.getExpiration(),
                    original.getRefreshPolicy(),
                    original.getMetadata()
                );
            case 1:
                return new CreateApiKeyRequest(
                    original.getName(),
                    Collections.singletonList(
                        Role.builder()
                            .name(randomAlphaOfLength(6))
                            .clusterPrivileges(ClusterPrivilegeName.ALL)
                            .indicesPrivileges(
                                IndicesPrivileges.builder().indices(randomAlphaOfLength(4)).privileges(IndexPrivilegeName.ALL).build()
                            )
                            .build()
                    ),
                    original.getExpiration(),
                    original.getRefreshPolicy(),
                    original.getMetadata()
                );
            case 2:
                return new CreateApiKeyRequest(
                    original.getName(),
                    original.getRoles(),
                    TimeValue.timeValueSeconds(10000),
                    original.getRefreshPolicy(),
                    original.getMetadata()
                );
            case 3:
                List<RefreshPolicy> values = Arrays.stream(RefreshPolicy.values())
                    .filter(rp -> rp != original.getRefreshPolicy())
                    .collect(Collectors.toList());
                return new CreateApiKeyRequest(
                    original.getName(),
                    original.getRoles(),
                    original.getExpiration(),
                    randomFrom(values),
                    original.getMetadata()
                );
            case 4:
                return new CreateApiKeyRequest(
                    original.getName(),
                    original.getRoles(),
                    original.getExpiration(),
                    original.getRefreshPolicy(),
                    randomValueOtherThan(original.getMetadata(), CreateApiKeyRequestTests::randomMetadata)
                );
            default:
                return new CreateApiKeyRequest(
                    randomAlphaOfLength(5),
                    original.getRoles(),
                    original.getExpiration(),
                    original.getRefreshPolicy(),
                    original.getMetadata()
                );
        }
    }

    @SuppressWarnings("unchecked")
    public static Map<String, Object> randomMetadata() {
        return randomFrom(
            org.elasticsearch.core.Map.of("status", "active", "level", 42, "nested", org.elasticsearch.core.Map.of("foo", "bar")),
            org.elasticsearch.core.Map.of("status", "active"),
            org.elasticsearch.core.Map.of(),
            null
        );
    }
}

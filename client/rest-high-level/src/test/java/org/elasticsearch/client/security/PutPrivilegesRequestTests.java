/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.security;

import org.elasticsearch.client.security.user.privileges.ApplicationPrivilege;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.equalTo;

public class PutPrivilegesRequestTests extends ESTestCase {

    public void testConstructor() {
        final List<ApplicationPrivilege> privileges = randomFrom(
            Arrays.asList(
                Collections.singletonList(
                    ApplicationPrivilege.builder()
                        .application("app01")
                        .privilege("all")
                        .actions(List.of("action:login", "action:logout"))
                        .metadata(Collections.singletonMap("k1", "v1"))
                        .build()
                ),
                null,
                Collections.emptyList()
            )
        );
        final RefreshPolicy refreshPolicy = randomFrom(RefreshPolicy.values());
        if (privileges == null || privileges.isEmpty()) {
            final IllegalArgumentException ile = expectThrows(
                IllegalArgumentException.class,
                () -> new PutPrivilegesRequest(privileges, refreshPolicy)
            );
            assertThat(ile.getMessage(), equalTo("privileges are required"));
        } else {
            final PutPrivilegesRequest putPrivilegesRequest = new PutPrivilegesRequest(privileges, refreshPolicy);
            assertThat(
                putPrivilegesRequest.getPrivileges().values().stream().flatMap(List::stream).collect(Collectors.toList()),
                equalTo(privileges)
            );
            assertThat(putPrivilegesRequest.getRefreshPolicy(), equalTo(refreshPolicy));
        }
    }

    public void testToXContent() throws IOException {
        final String expected = """
            {
              "app01" : {
                "all" : {
                  "application" : "app01",
                  "name" : "all",
                  "actions" : [
                    "action:login",
                    "action:logout"
                  ],
                  "metadata" : {
                    "k1" : "v1"
                  }
                },
                "read" : {
                  "application" : "app01",
                  "name" : "read",
                  "actions" : [
                    "data:read"
                  ]
                }
              },
              "app02" : {
                "all" : {
                  "application" : "app02",
                  "name" : "all",
                  "actions" : [
                    "action:login",
                    "action:logout"
                  ],
                  "metadata" : {
                    "k2" : "v2"
                  }
                }
              }
            }""";
        List<ApplicationPrivilege> privileges = new ArrayList<>();
        privileges.add(
            ApplicationPrivilege.builder()
                .application("app01")
                .privilege("all")
                .actions(List.of("action:login", "action:logout"))
                .metadata(Collections.singletonMap("k1", "v1"))
                .build()
        );
        privileges.add(ApplicationPrivilege.builder().application("app01").privilege("read").actions(List.of("data:read")).build());
        privileges.add(
            ApplicationPrivilege.builder()
                .application("app02")
                .privilege("all")
                .actions(List.of("action:login", "action:logout"))
                .metadata(Collections.singletonMap("k2", "v2"))
                .build()
        );
        final RefreshPolicy refreshPolicy = randomFrom(RefreshPolicy.values());
        final PutPrivilegesRequest putPrivilegesRequest = new PutPrivilegesRequest(privileges, refreshPolicy);
        final XContentBuilder builder = XContentFactory.jsonBuilder().prettyPrint();
        assertThat(Strings.toString(putPrivilegesRequest.toXContent(builder, ToXContent.EMPTY_PARAMS)), equalTo(expected));
    }

    public void testEqualsHashCode() {
        final List<ApplicationPrivilege> privileges = new ArrayList<>();
        privileges.add(
            ApplicationPrivilege.builder()
                .application(randomAlphaOfLength(5))
                .privilege(randomAlphaOfLength(3))
                .actions(List.of(randomAlphaOfLength(5), randomAlphaOfLength(5)))
                .metadata(Collections.singletonMap(randomAlphaOfLength(3), randomAlphaOfLength(3)))
                .build()
        );
        privileges.add(
            ApplicationPrivilege.builder()
                .application(randomAlphaOfLength(5))
                .privilege(randomAlphaOfLength(3))
                .actions(List.of(randomAlphaOfLength(5), randomAlphaOfLength(5)))
                .metadata(Collections.singletonMap(randomAlphaOfLength(3), randomAlphaOfLength(3)))
                .build()
        );
        final RefreshPolicy refreshPolicy = randomFrom(RefreshPolicy.values());
        PutPrivilegesRequest putPrivilegesRequest = new PutPrivilegesRequest(privileges, refreshPolicy);

        EqualsHashCodeTestUtils.checkEqualsAndHashCode(
            putPrivilegesRequest,
            (original) -> { return new PutPrivilegesRequest(privileges, refreshPolicy); }
        );
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(putPrivilegesRequest, (original) -> {
            return new PutPrivilegesRequest(
                original.getPrivileges().values().stream().flatMap(List::stream).collect(Collectors.toList()),
                original.getRefreshPolicy()
            );
        }, PutPrivilegesRequestTests::mutateTestItem);
    }

    private static PutPrivilegesRequest mutateTestItem(PutPrivilegesRequest original) {
        final Set<RefreshPolicy> policies = Sets.newHashSet(RefreshPolicy.values());
        policies.remove(original.getRefreshPolicy());
        switch (randomIntBetween(0, 1)) {
            case 0:
                final List<ApplicationPrivilege> privileges = new ArrayList<>();
                privileges.add(
                    ApplicationPrivilege.builder()
                        .application(randomAlphaOfLength(5))
                        .privilege(randomAlphaOfLength(3))
                        .actions(List.of(randomAlphaOfLength(6)))
                        .build()
                );
                return new PutPrivilegesRequest(privileges, original.getRefreshPolicy());
            case 1:
                return new PutPrivilegesRequest(
                    original.getPrivileges().values().stream().flatMap(List::stream).collect(Collectors.toList()),
                    randomFrom(policies)
                );
            default:
                return new PutPrivilegesRequest(
                    original.getPrivileges().values().stream().flatMap(List::stream).collect(Collectors.toList()),
                    randomFrom(policies)
                );
        }
    }
}

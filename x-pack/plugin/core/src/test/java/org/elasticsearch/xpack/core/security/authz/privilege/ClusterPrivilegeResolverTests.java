/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authz.privilege;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xpack.core.security.action.apikey.CreateCrossClusterApiKeyAction;
import org.elasticsearch.xpack.core.security.action.apikey.UpdateCrossClusterApiKeyAction;
import org.elasticsearch.xpack.core.security.authc.AuthenticationTestHelper;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.SortedMap;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.startsWith;
import static org.mockito.Mockito.mock;

public class ClusterPrivilegeResolverTests extends ESTestCase {

    public void testSortByAccessLevel() {
        final List<NamedClusterPrivilege> privileges = new ArrayList<>(
            List.of(
                ClusterPrivilegeResolver.ALL,
                ClusterPrivilegeResolver.MONITOR,
                ClusterPrivilegeResolver.MANAGE,
                ClusterPrivilegeResolver.MANAGE_OWN_API_KEY,
                ClusterPrivilegeResolver.MANAGE_API_KEY,
                ClusterPrivilegeResolver.READ_SECURITY,
                ClusterPrivilegeResolver.MANAGE_SECURITY
            )
        );
        Collections.shuffle(privileges, random());
        final SortedMap<String, NamedClusterPrivilege> sorted = ClusterPrivilegeResolver.sortByAccessLevel(privileges);
        // This is:
        // "manage_own_api_key", "monitor", "read_security" (neither of which grant anything else in the list), sorted by name
        // "manage" and "manage_api_key",(which each grant 1 other privilege in the list), sorted by name
        // "manage_security" and "all", sorted by access level ("all" implies "manage_security")
        assertThat(
            sorted.keySet(),
            contains("manage_own_api_key", "monitor", "read_security", "manage", "manage_api_key", "manage_security", "all")
        );
    }

    public void testDataStreamActionsNotGrantedByAllClusterPrivilege() {
        ClusterPrivilegeResolver.ALL.permission()
            .privileges()
            .forEach(
                p -> ((ActionClusterPrivilege) p).getAllowedActionPatterns()
                    .forEach(pattern -> assertThat(pattern, not(startsWith("indices:admin/data_stream/"))))
            );

        assertThat(
            ClusterPrivilegeResolver.ALL.permission()
                .check(
                    "indices:admin/data_stream/" + randomAlphaOfLengthBetween(0, 10),
                    mock(TransportRequest.class),
                    AuthenticationTestHelper.builder().build()
                ),
            is(false)
        );
    }

    public void testPrivilegesForCreateAndUpdateCrossClusterApiKey() {
        assertThat(
            ClusterPrivilegeResolver.MANAGE_API_KEY.permission()
                .check(CreateCrossClusterApiKeyAction.NAME, mock(TransportRequest.class), AuthenticationTestHelper.builder().build()),
            is(false)
        );

        assertThat(
            ClusterPrivilegeResolver.MANAGE_API_KEY.permission()
                .check(UpdateCrossClusterApiKeyAction.NAME, mock(TransportRequest.class), AuthenticationTestHelper.builder().build()),
            is(false)
        );

        assertThat(
            ClusterPrivilegeResolver.MANAGE_SECURITY.permission()
                .check(CreateCrossClusterApiKeyAction.NAME, mock(TransportRequest.class), AuthenticationTestHelper.builder().build()),
            is(true)
        );

        assertThat(
            ClusterPrivilegeResolver.MANAGE_SECURITY.permission()
                .check(UpdateCrossClusterApiKeyAction.NAME, mock(TransportRequest.class), AuthenticationTestHelper.builder().build()),
            is(true)
        );
    }
}

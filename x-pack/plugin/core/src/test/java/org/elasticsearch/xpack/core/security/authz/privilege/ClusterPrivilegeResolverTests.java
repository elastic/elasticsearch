/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authz.privilege;

import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.SortedMap;

import static org.hamcrest.Matchers.contains;

public class ClusterPrivilegeResolverTests extends ESTestCase {

    public void testSortByAccessLevel() throws Exception {
        final List<NamedClusterPrivilege> privileges = new ArrayList<>(
            List.of(
                ClusterPrivilegeResolver.ALL,
                ClusterPrivilegeResolver.MONITOR,
                ClusterPrivilegeResolver.MANAGE,
                ClusterPrivilegeResolver.MANAGE_OWN_API_KEY,
                ClusterPrivilegeResolver.MANAGE_API_KEY,
                ClusterPrivilegeResolver.MANAGE_SECURITY
            )
        );
        Collections.shuffle(privileges, random());
        final SortedMap<String, NamedClusterPrivilege> sorted = ClusterPrivilegeResolver.sortByAccessLevel(privileges);
        // This is:
        // "manage_own_api_key", "monitor" (neither of which grant anything else in the list), sorted by name
        // "manage" and "manage_api_key",(which each grant 1 other privilege in the list), sorted by name
        // "manage_security" and "all", sorted by access level ("all" implies "manage_security")
        assertThat(sorted.keySet(), contains("manage_own_api_key", "monitor", "manage", "manage_api_key", "manage_security", "all"));
    }

}

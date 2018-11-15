/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.security.authz.permission;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.authz.privilege.ApplicationPrivilege;
import org.elasticsearch.xpack.core.security.authz.privilege.IndexPrivilege;
import org.elasticsearch.xpack.core.security.authz.privilege.Privilege;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class RoleTests extends ESTestCase {

    public void testIsSubsetOfThrowsErrorForNullRole() {
        final Tuple<IndexPrivilege, Set<String>> indicesPrivileges = new Tuple<IndexPrivilege, Set<String>>(IndexPrivilege.ALL,
                Sets.newHashSet("test-1*"));
        final Privilege runAsPrivilege = new Privilege("name", "user1", "run*");
        final Role subsetRole = buildRole(new String[] { "a" }, Sets.newHashSet("monitor"), runAsPrivilege, null,
                indicesPrivileges, null);
        final NullPointerException npe = expectThrows(NullPointerException.class, () -> subsetRole.isSubsetOf(null));
        assertThat(npe.getMessage(), equalTo("other role is required for subset checks"));
    }

    public void testIsSubsetOfWhereTheResultIsYes() {
        final Map<ApplicationPrivilege, Set<String>> baseRoleApplicationPrivileges = buildApplicationPrivilege("app-1", "act-name-1",
                new String[] { "DATA:read/*", "ACTION:act-name-1" }, Sets.newHashSet("res1", "res2"));
        baseRoleApplicationPrivileges.putAll(buildApplicationPrivilege("app-2", "act-name-2",
                new String[] { "DATA:read/*", "ACTION:act-name-2" }, Sets.newHashSet("res1", "res2")));
        baseRoleApplicationPrivileges.putAll(buildApplicationPrivilege("app-1", "act-name-2",
                new String[] { "DATA:write/*", "ACTION:act-name-2" }, Sets.newHashSet("res2")));

        final Privilege baseRoleRunAsPrivilege = new Privilege("name", "user*", "run*");
        final Tuple<IndexPrivilege, Set<String>> baseRoleIndicesPrivileges = new Tuple<IndexPrivilege, Set<String>>(IndexPrivilege.ALL,
                Sets.newHashSet("test-*"));
        final Role baseRole = buildRole(new String[] { "a", "b" }, Sets.newHashSet("all"), baseRoleRunAsPrivilege,
                baseRoleApplicationPrivileges, baseRoleIndicesPrivileges, null);

        final Map<ApplicationPrivilege, Set<String>> role1AppPriv = buildApplicationPrivilege("app-1", "act-name-1",
                new String[] { "DATA:read/X/*" }, Sets.newHashSet("res1"));
        role1AppPriv.putAll(buildApplicationPrivilege("app-1", "act-name-2", new String[] { "DATA:write/Y/*" }, Sets.newHashSet("res2")));

        final Privilege runAsPrivilege = new Privilege("name", "user1", "run*");
        final Tuple<IndexPrivilege, Set<String>> indicesPrivileges = new Tuple<IndexPrivilege, Set<String>>(IndexPrivilege.ALL,
                Sets.newHashSet("test-1*"));
        final Role subsetRole = buildRole(new String[] { "a" }, Sets.newHashSet("monitor"), runAsPrivilege, role1AppPriv,
                indicesPrivileges, null);

        SubsetResult result = subsetRole.isSubsetOf(baseRole);
        assertThat(result.result(), is(SubsetResult.Result.YES));
    }

    public void testIsSubsetOfWhereTheResultIsNo() {
        final Map<ApplicationPrivilege, Set<String>> baseRoleApplicationPrivileges = buildApplicationPrivilege("app-1", "act-name-1",
                new String[] { "DATA:read/*", "ACTION:act-name-1" }, Sets.newHashSet("res1", "res2"));

        final Privilege baseRoleRunAsPrivilege = new Privilege("name", "user*", "run*");
        final Tuple<IndexPrivilege, Set<String>> baseRoleIndicesPrivileges = new Tuple<IndexPrivilege, Set<String>>(IndexPrivilege.ALL,
                Sets.newHashSet("test-*"));
        final Role baseRole = buildRole(new String[] { "a", "b" }, Sets.newHashSet("all"), baseRoleRunAsPrivilege,
                baseRoleApplicationPrivileges, baseRoleIndicesPrivileges, null);

        final Map<ApplicationPrivilege, Set<String>> role1AppPriv = buildApplicationPrivilege("app-1", "act-name-1",
                new String[] { "DATA:write/X/*" }, Sets.newHashSet("res1"));

        final Privilege runAsPrivilege = new Privilege("name", "user1", "run*");
        final Tuple<IndexPrivilege, Set<String>> indicesPrivileges = new Tuple<IndexPrivilege, Set<String>>(IndexPrivilege.ALL,
                Sets.newHashSet("test-1*"));
        final Role subsetRole = buildRole(new String[] { "a" }, Sets.newHashSet("monitor"), runAsPrivilege, role1AppPriv,
                indicesPrivileges, null);

        SubsetResult result = subsetRole.isSubsetOf(baseRole);
        assertThat(result.result(), is(SubsetResult.Result.NO));
    }

    public void testIsSubsetOfWhereTheResultIsMaybe() {
        final Map<ApplicationPrivilege, Set<String>> baseRoleApplicationPrivileges = buildApplicationPrivilege("app-1", "act-name-1",
                new String[] { "DATA:read/*", "ACTION:act-name-1" }, Sets.newHashSet("res1", "res2"));

        final Privilege baseRoleRunAsPrivilege = new Privilege("name", "user*", "run*");
        final Tuple<IndexPrivilege, Set<String>> baseRoleIndicesPrivileges = new Tuple<IndexPrivilege, Set<String>>(IndexPrivilege.ALL,
                Sets.newHashSet("test-*"));
        final Role baseRole = buildRole(new String[] { "a", "b" }, Sets.newHashSet("all"), baseRoleRunAsPrivilege,
                baseRoleApplicationPrivileges, baseRoleIndicesPrivileges, new BytesArray("base-query"));

        final Map<ApplicationPrivilege, Set<String>> role1AppPriv = buildApplicationPrivilege("app-1", "act-name-1",
                new String[] { "DATA:read/X/*" }, Sets.newHashSet("res1"));

        final Privilege runAsPrivilege = new Privilege("name", "user1", "run*");
        final Tuple<IndexPrivilege, Set<String>> indicesPrivileges = new Tuple<IndexPrivilege, Set<String>>(IndexPrivilege.ALL,
                Sets.newHashSet("test-1*"));
        final Role subsetRole = buildRole(new String[] { "a" }, Sets.newHashSet("monitor"), runAsPrivilege, role1AppPriv,
                indicesPrivileges, new BytesArray("query"));

        SubsetResult result = subsetRole.isSubsetOf(baseRole);
        assertThat(result.result(), is(SubsetResult.Result.MAYBE));
        Set<Set<String>> expected = Collections.singleton(Sets.newHashSet("test-1*"));
        assertThat(result.setOfIndexNamesForCombiningDLSQueries(), equalTo(expected));
    }

    private Role buildRole(String[] names, Set<String> clusterPrivileges, Privilege runAsPermission,
            Map<ApplicationPrivilege, Set<String>> applicationPrivilegesResources, Tuple<IndexPrivilege, Set<String>> indexPrivIndices,
            BytesReference query) {
        final Role.Builder roleBuilder = Role.builder(names).cluster(clusterPrivileges, new ArrayList<>());
        if (applicationPrivilegesResources != null) {
            for (Map.Entry<ApplicationPrivilege, Set<String>> entry : applicationPrivilegesResources.entrySet()) {
                roleBuilder.addApplicationPrivilege(entry.getKey(), entry.getValue());
            }
        }
        roleBuilder.runAs(runAsPermission);
        if (query == null) {
            roleBuilder.add(indexPrivIndices.v1(), indexPrivIndices.v2().toArray(new String[0]));
        } else {
            roleBuilder.add(FieldPermissions.DEFAULT, Collections.singleton(query), indexPrivIndices.v1(),
                    indexPrivIndices.v2().toArray(new String[0]));
        }
        return roleBuilder.build();
    }

    private Map<ApplicationPrivilege, Set<String>> buildApplicationPrivilege(String app, String name, String[] actions,
            Set<String> resources) {
        final ApplicationPrivilege priv = defineApplicationPrivilege(app, name, actions);
        final Map<ApplicationPrivilege, Set<String>> appPrivResources = new HashMap<>();
        appPrivResources.put(priv, resources);
        return appPrivResources;
    }

    private ApplicationPrivilege defineApplicationPrivilege(String app, String name, String... actions) {
        return new ApplicationPrivilege(app, name, actions);
    }
}

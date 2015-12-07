/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authz;

import org.elasticsearch.action.get.GetAction;
import org.elasticsearch.shield.authz.Privilege.Cluster;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.function.Predicate;

import static org.elasticsearch.shield.authz.Privilege.Index.MONITOR;
import static org.elasticsearch.shield.authz.Privilege.Index.READ;
import static org.elasticsearch.shield.authz.Privilege.Index.SEARCH;
import static org.elasticsearch.shield.authz.Privilege.Index.union;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

/**
 *
 */
public class PermissionTests extends ESTestCase {
    private Permission.Global.Role permission;

    @Before
    public void init() {
        Permission.Global.Role.Builder builder = Permission.Global.Role.builder("test");
        builder.add(union(SEARCH, MONITOR), "test_*", "/foo.*/");
        builder.add(union(READ), "baz_*foo", "/fool.*bar/");
        builder.add(union(MONITOR), "/bar.*/");
        permission = builder.build();
    }

    public void testAllowedIndicesMatcherAction() throws Exception {
        testAllowedIndicesMatcher(permission.indices().allowedIndicesMatcher(GetAction.NAME));
    }

    public void testAllowedIndicesMatcherActionCaching() throws Exception {
        Predicate<String> matcher1 = permission.indices().allowedIndicesMatcher(GetAction.NAME);
        Predicate<String> matcher2 = permission.indices().allowedIndicesMatcher(GetAction.NAME);
        assertThat(matcher1, is(matcher2));
    }

    public void testIndicesGlobalsIterator() {
        Permission.Global.Role.Builder builder = Permission.Global.Role.builder("tc_role");
        builder.cluster(Cluster.action("cluster:monitor/nodes/info"));
        Permission.Global.Role noIndicesPermission = builder.build();

        Permission.Indices.Globals indicesGlobals = new Permission.Indices.Globals(Collections.<Permission.Global>unmodifiableList(Arrays.asList(noIndicesPermission, permission)));
        Iterator<Permission.Indices.Group> iterator = indicesGlobals.iterator();
        assertThat(iterator.hasNext(), is(equalTo(true)));
        int count = 0;
        while (iterator.hasNext()) {
            iterator.next();
            count++;
        }
        assertThat(count, is(equalTo(permission.indices().groups().length)));
    }

    public void testBuildEmptyRole() {
        Permission.Global.Role.Builder permission = Permission.Global.Role.builder("some_role");
        Permission.Global.Role role = permission.build();
        assertThat(role, notNullValue());
        assertThat(role.cluster(), notNullValue());
        assertThat(role.indices(), notNullValue());
        assertThat(role.runAs(), notNullValue());
    }

    public void testRunAs() {
        Permission.Global.Role permission = Permission.Global.Role.builder("some_role")
                .runAs(new Privilege.General("name", "user1", "run*"))
                .build();
        assertThat(permission.runAs().check("user1"), is(true));
        assertThat(permission.runAs().check("user"), is(false));
        assertThat(permission.runAs().check("run" + randomAsciiOfLengthBetween(1, 10)), is(true));
    }

    // "baz_*foo", "/fool.*bar/"
    private void testAllowedIndicesMatcher(Predicate<String> indicesMatcher) {
        assertThat(indicesMatcher.test("foobar"), is(false));
        assertThat(indicesMatcher.test("fool"), is(false));
        assertThat(indicesMatcher.test("fool2bar"), is(true));
        assertThat(indicesMatcher.test("baz_foo"), is(true));
        assertThat(indicesMatcher.test("barbapapa"), is(false));
    }
}

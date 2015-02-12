/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authz;

import org.elasticsearch.action.get.GetAction;
import org.elasticsearch.common.base.Predicate;
import org.elasticsearch.common.collect.ImmutableList;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Before;
import org.junit.Test;

import java.util.Iterator;

import static org.elasticsearch.shield.authz.Privilege.Index.*;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

/**
 *
 */
public class PermissionTests extends ElasticsearchTestCase {

    private Permission.Global.Role permission;

    @Before
    public void init() {
        Permission.Global.Role.Builder builder = Permission.Global.Role.builder("test");
        builder.add(union(SEARCH, MONITOR), "test_*", "/foo.*/");
        builder.add(union(READ), "baz_*foo", "/fool.*bar/");
        builder.add(union(MONITOR), "/bar.*/");
        permission = builder.build();
    }

    @Test
    public void testAllowedIndicesMatcher_Action() throws Exception {
        testAllowedIndicesMatcher(permission.indices().allowedIndicesMatcher(GetAction.NAME));
    }

    @Test
    public void testAllowedIndicesMatcher_Action_Caching() throws Exception {
        Predicate<String> matcher1 = permission.indices().allowedIndicesMatcher(GetAction.NAME);
        Predicate<String> matcher2 = permission.indices().allowedIndicesMatcher(GetAction.NAME);
        assertThat(matcher1, is(matcher2));
    }

    @Test
    public void testIndicesGlobalsIterator() {
        Permission.Global.Role.Builder builder = Permission.Global.Role.builder("tc_role");
        builder.set(Cluster.action("cluster:monitor/nodes/info"));
        Permission.Global.Role noIndicesPermission = builder.build();

        Permission.Indices.Globals indicesGlobals = new Permission.Indices.Globals(ImmutableList.<Permission.Global>of(noIndicesPermission, permission));
        Iterator<Permission.Indices.Group> iterator = indicesGlobals.iterator();
        assertThat(iterator.hasNext(), is(equalTo(true)));
        int count = 0;
        while (iterator.hasNext()) {
            iterator.next();
            count++;
        }
        assertThat(count, is(equalTo(permission.indices().groups().length)));
    }

    // "baz_*foo", "/fool.*bar/"
    private void testAllowedIndicesMatcher(Predicate<String> indicesMatcher) {
        assertThat(indicesMatcher.apply("foobar"), is(false));
        assertThat(indicesMatcher.apply("fool"), is(false));
        assertThat(indicesMatcher.apply("fool2bar"), is(true));
        assertThat(indicesMatcher.apply("baz_foo"), is(true));
        assertThat(indicesMatcher.apply("barbapapa"), is(false));
    }


}

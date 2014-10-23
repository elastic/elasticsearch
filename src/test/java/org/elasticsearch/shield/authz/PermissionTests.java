/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authz;

import org.elasticsearch.action.get.GetAction;
import org.elasticsearch.common.base.Predicate;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Before;
import org.junit.Test;

import static org.elasticsearch.shield.authz.Privilege.Index.*;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

/**
 *
 */
public class PermissionTests extends ElasticsearchTestCase {

    private Permission.Global permission;

    @Before
    public void init() {
        Permission.Global.Builder builder = Permission.Global.builder(mock(AuthorizationService.class));
        builder.add(union(SEARCH, MONITOR), "test_*", "/foo.*/");
        builder.add(union(READ), "baz_*foo", "/fool.*bar/");
        builder.add(union(MONITOR), "/bar.*/");
        permission = builder.build();
    }

    @Test
    public void testAllowedIndicesMatcher_Privilege() throws Exception {
        testAllowedIndicesMatcher(permission.indices().allowedIndicesMatcher(GET));
    }

    @Test
    public void testAllowedIndicesMatcher_Privilege_Caching() throws Exception {
        Predicate<String> matcher1 = permission.indices().allowedIndicesMatcher(GET.plus(SEARCH).plus(WRITE));
        Predicate<String> matcher2 = permission.indices().allowedIndicesMatcher(GET.plus(SEARCH).plus(WRITE));
        assertThat(matcher1, is(matcher2));
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

    private void testAllowedIndicesMatcher(Predicate<String> indicesMatcher) {
        assertThat(indicesMatcher.apply("test_123"), is(true));
        assertThat(indicesMatcher.apply("foobar"), is(true));
        assertThat(indicesMatcher.apply("fool"), is(true));
        assertThat(indicesMatcher.apply("fool2bar"), is(true));
        assertThat(indicesMatcher.apply("barbapapa"), is(false));
    }


}

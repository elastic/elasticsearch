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
import static org.hamcrest.Matchers.*;

/**
 *
 */
public class PermissionTests extends ElasticsearchTestCase {

    private Permission.Global permission;

    @Before
    public void init() {
        Permission.Global.Builder builder = Permission.Global.builder();
        builder.add(union(SEARCH, MONITOR), "test_.*", "foo.*");
        builder.add(union(READ), "baz_.*foo", "fool.*bar");
        builder.add(union(MONITOR), "bar.*");
        permission = builder.build();
    }

    @Test
    public void testAllowedIndicesMatcher_Privilege() throws Exception {
        testAllowedIndicesMatcher(permission.indices().allowedIndicesMatcher(GET));
    }

    @Test
    public void testAllowedIndicesMatcher_Action() throws Exception {
        testAllowedIndicesMatcher(permission.indices().allowedIndicesMatcher(GetAction.NAME));
    }

    private void testAllowedIndicesMatcher(Predicate<String> indicesMatcher) {
        assertThat(indicesMatcher.apply("test_123"), is(true));
        assertThat(indicesMatcher.apply("foobar"), is(true));
        assertThat(indicesMatcher.apply("fool"), is(true));
        assertThat(indicesMatcher.apply("fool2bar"), is(true));
        assertThat(indicesMatcher.apply("barbapapa"), is(false));
    }


}

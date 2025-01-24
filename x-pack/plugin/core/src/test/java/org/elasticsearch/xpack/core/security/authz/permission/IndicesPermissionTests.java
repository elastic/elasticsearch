/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authz.permission;

import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;

public class IndicesPermissionTests extends ESTestCase {

    public void testConvertToExcludeFailures() {

        // trailing wildcard
        assertThat(IndicesPermission.Group.convertToExcludeFailures("foo*"), Matchers.equalTo("/(foo.*)&~(foo.*::failures)/"));

        // regular expression
        assertThat(IndicesPermission.Group.convertToExcludeFailures("/foo.*/"), Matchers.equalTo("/(foo.*)&~(foo.*::failures)/"));

    }

    public void testMaybeAddFailureExclusions() {
        String[] indices = new String[] { "foo*", "/foo.*/" };
        String[] result = IndicesPermission.Group.maybeAddFailureExclusions(indices);
        // System.out.println(java.util.Arrays.toString(result));
        assertThat(result[0], Matchers.equalTo("/(foo.*)&~(foo.*::failures)/"));
        assertThat(result[0], Matchers.equalTo(result[1]));

    }
}

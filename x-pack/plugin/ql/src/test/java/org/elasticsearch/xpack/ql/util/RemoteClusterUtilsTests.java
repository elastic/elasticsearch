/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ql.util;

import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.ESTestCase;

import static org.elasticsearch.xpack.ql.util.StringUtils.isQualified;
import static org.elasticsearch.xpack.ql.util.StringUtils.qualifyAndJoinIndices;
import static org.elasticsearch.xpack.ql.util.StringUtils.splitQualifiedIndex;

public class RemoteClusterUtilsTests extends ESTestCase {
    public void testSplitQualifiedIndex() {
        String cluster = randomAlphaOfLength(20);
        String index = randomAlphaOfLength(30);
        assertEquals(Tuple.tuple(cluster, index), splitQualifiedIndex(cluster + ":" + index));
    }

    public void testQualifyAndJoinIndices() {
        String[] indices = { "foo", "bar", "bar*", "*foo" };
        assertEquals("cluster:foo,cluster:bar,cluster:bar*,cluster:*foo", qualifyAndJoinIndices("cluster", indices));
    }

    public void testIsQualified() {
        assertTrue(isQualified("foo:bar"));
        assertFalse(isQualified("foobar"));
    }
}

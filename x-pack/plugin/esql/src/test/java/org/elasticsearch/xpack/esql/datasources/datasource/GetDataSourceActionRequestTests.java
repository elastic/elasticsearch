/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.datasource;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.datasource.GetDataSourceAction.Request;

/** Unit tests for {@link Request}. */
public class GetDataSourceActionRequestTests extends ESTestCase {

    public void testConstructorRejectsNullNames() {
        expectThrows(NullPointerException.class, () -> new Request(TEST_REQUEST_TIMEOUT, null));
    }

    public void testEqualsAndHashCodeForEqualRequests() {
        Request a = new Request(TEST_REQUEST_TIMEOUT, new String[] { "ds_a", "ds_b" });
        Request b = new Request(TEST_REQUEST_TIMEOUT, new String[] { "ds_a", "ds_b" });
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
    }

    public void testEqualsDifferentNames() {
        Request a = new Request(TEST_REQUEST_TIMEOUT, new String[] { "ds_a" });
        Request b = new Request(TEST_REQUEST_TIMEOUT, new String[] { "ds_b" });
        assertNotEquals(a, b);
    }
}

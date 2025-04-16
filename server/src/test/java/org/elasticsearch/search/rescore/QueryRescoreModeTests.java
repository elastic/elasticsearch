/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.rescore;

import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

/**
 * Test fixing the ordinals and names in {@link QueryRescoreMode}. These should not be changed since we
 * use the names in the parser and the ordinals in serialization.
 */
public class QueryRescoreModeTests extends ESTestCase {

    /**
     * Test @link {@link QueryRescoreMode} enum ordinals and names, since serialization relies on it
     */
    public void testQueryRescoreMode() throws IOException {
        float primary = randomFloat();
        float secondary = randomFloat();
        assertEquals(0, QueryRescoreMode.Avg.ordinal());
        assertEquals("avg", QueryRescoreMode.Avg.toString());
        assertEquals((primary + secondary) / 2.0f, QueryRescoreMode.Avg.combine(primary, secondary), Float.MIN_VALUE);

        assertEquals(1, QueryRescoreMode.Max.ordinal());
        assertEquals("max", QueryRescoreMode.Max.toString());
        assertEquals(Math.max(primary, secondary), QueryRescoreMode.Max.combine(primary, secondary), Float.MIN_VALUE);

        assertEquals(2, QueryRescoreMode.Min.ordinal());
        assertEquals("min", QueryRescoreMode.Min.toString());
        assertEquals(Math.min(primary, secondary), QueryRescoreMode.Min.combine(primary, secondary), Float.MIN_VALUE);

        assertEquals(3, QueryRescoreMode.Total.ordinal());
        assertEquals("sum", QueryRescoreMode.Total.toString());
        assertEquals(primary + secondary, QueryRescoreMode.Total.combine(primary, secondary), Float.MIN_VALUE);

        assertEquals(4, QueryRescoreMode.Multiply.ordinal());
        assertEquals("product", QueryRescoreMode.Multiply.toString());
        assertEquals(primary * secondary, QueryRescoreMode.Multiply.combine(primary, secondary), Float.MIN_VALUE);
    }
}

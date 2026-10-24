/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.test.ESTestCase;

/** Selecting the experimental translator must preserve the query's existing resource limits. */
public class EsqlFlagsTests extends ESTestCase {
    public void testFunctionalMetadataPreservesOtherFlags() {
        EsqlFlags original = new EsqlFlags(false, 23, 17, 7, 11, 41, 777);
        EsqlFlags enabled = original.withPromqlFunctionalTimeSeries(true);

        assertTrue(enabled.promqlFunctionalTimeSeries());
        assertEquals(777, enabled.maxCommandCount());
        assertEquals(original, enabled.withPromqlFunctionalTimeSeries(false));
        assertFalse(EsqlFlags.DEFAULTS.promqlFunctionalTimeSeries());
    }
}

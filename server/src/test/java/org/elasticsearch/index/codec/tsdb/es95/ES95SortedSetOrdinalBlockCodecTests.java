/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.es95;

import org.elasticsearch.index.codec.tsdb.SortedSetOrdinalBlockCodec;
import org.elasticsearch.index.codec.tsdb.TSDBSortedSetOrdinalBlockCodec;
import org.elasticsearch.test.ESTestCase;

/**
 * Lightweight tests for {@link ES95SortedSetOrdinalBlockCodec}, the per-field-type
 * specialization that drives ES95's adaptive ordinal encoding on SORTED_SET fields.
 * End-to-end correctness against real segment contexts is exercised in the doc
 * values format compliance suite.
 */
public class ES95SortedSetOrdinalBlockCodecTests extends ESTestCase {

    public void testExtendsLegacyBaselineAndImplementsSortedSetInterface() {
        final ES95SortedSetOrdinalBlockCodec codec = new ES95SortedSetOrdinalBlockCodec();
        assertNotNull(codec);
        final TSDBSortedSetOrdinalBlockCodec baselineRef = codec;
        assertSame(codec, baselineRef);
        final SortedSetOrdinalBlockCodec interfaceRef = codec;
        assertSame(codec, interfaceRef);
    }
}

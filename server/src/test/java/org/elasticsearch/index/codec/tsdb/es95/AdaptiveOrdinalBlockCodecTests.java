/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.es95;

import org.elasticsearch.index.codec.tsdb.SortedOrdinalBlockCodec;
import org.elasticsearch.index.codec.tsdb.SortedSetOrdinalBlockCodec;
import org.elasticsearch.test.ESTestCase;

/**
 * Lightweight tests for {@link AdaptiveOrdinalBlockCodec}, the per-field-type ordinal
 * block codec that connects this package's adaptive per-block codec to ES95's TSDB doc
 * values pipeline. End-to-end correctness against real segment contexts is exercised in
 * the doc-values format compliance suite.
 */
public class AdaptiveOrdinalBlockCodecTests extends ESTestCase {

    public void testImplementsBothOrdinalBlockCodecInterfaces() {
        // NOTE: type bounds are checked by the assignments; the assertions just ensure
        // the class continues to declare both interface implementations.
        final AdaptiveOrdinalBlockCodec codec = new AdaptiveOrdinalBlockCodec();
        assertNotNull(codec);
        final SortedOrdinalBlockCodec sortedRef = codec;
        assertSame(codec, sortedRef);
        final SortedSetOrdinalBlockCodec sortedSetRef = codec;
        assertSame(codec, sortedSetRef);
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.es95;

import org.elasticsearch.index.codec.tsdb.OrdinalBlockCodec;
import org.elasticsearch.test.ESTestCase;

/**
 * Lightweight tests for {@link AdaptiveOrdinalBlockCodec}, the
 * {@link OrdinalBlockCodec} implementation that connects this package's
 * adaptive per-block codec to ES95's TSDB doc values pipeline. End-to-end
 * correctness against real segment contexts is exercised in the doc-values
 * format compliance suite.
 */
public class AdaptiveOrdinalBlockCodecTests extends ESTestCase {

    public void testImplementsOrdinalBlockCodec() {
        // NOTE: type bound is checked by the assignment; the assertion just
        // ensures the class continues to declare the interface implementation.
        final AdaptiveOrdinalBlockCodec codec = new AdaptiveOrdinalBlockCodec();
        assertNotNull(codec);
        final OrdinalBlockCodec interfaceRef = codec;
        assertSame(codec, interfaceRef);
    }
}

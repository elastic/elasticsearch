/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.pipeline;

import java.io.IOException;

public class IntegerCompressionOverheadTests extends CompressionOverheadTestCase {

    public void testTimestampData() throws IOException {
        assertMaxOneByteOverhead("timestamp", timestampData());
    }

    public void testCounterData() throws IOException {
        assertMaxOneByteOverhead("counter", counterData());
    }

    public void testGaugeData() throws IOException {
        assertMaxOneByteOverhead("gauge", gaugeData());
    }

    public void testGcdData() throws IOException {
        assertMaxOneByteOverhead("gcd", gcdData());
    }

    public void testConstantData() throws IOException {
        assertMaxOneByteOverhead("constant", constantData());
    }

    public void testRandomData() throws IOException {
        assertMaxOneByteOverhead("random", randomData());
    }

    public void testSmallData() throws IOException {
        assertMaxOneByteOverhead("small", smallData());
    }
}

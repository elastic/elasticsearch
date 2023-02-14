/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.utils;

import org.elasticsearch.test.ESTestCase;

public class OutputFieldNameConverterTests extends ESTestCase {

    public void testFromDouble() {
        assertEquals("42_42", OutputFieldNameConverter.fromDouble(42.42));
        // remove '.0' if possible
        assertEquals("42", OutputFieldNameConverter.fromDouble(42.0));
        // digit limit
        assertEquals("42_42424242424242", OutputFieldNameConverter.fromDouble(42.4242424242424242424242424242424242));
        // scientific notation keep the '.0'
        assertEquals("1_0E-100", OutputFieldNameConverter.fromDouble(1.0E-100));
        // scientific with digits
        assertEquals("1_12345E-100", OutputFieldNameConverter.fromDouble(1.12345E-100));
        // NaN (OutputFieldNameConverter clients should disallow that)
        assertEquals("NaN", OutputFieldNameConverter.fromDouble(Double.NaN));
        // infinity
        assertEquals("-Infinity", OutputFieldNameConverter.fromDouble(Double.NEGATIVE_INFINITY));
        assertEquals("Infinity", OutputFieldNameConverter.fromDouble(Double.POSITIVE_INFINITY));
    }
}

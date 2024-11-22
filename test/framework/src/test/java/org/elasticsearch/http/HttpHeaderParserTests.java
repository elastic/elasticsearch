/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.http;

import org.elasticsearch.test.ESTestCase;

import java.math.BigInteger;

public class HttpHeaderParserTests extends ESTestCase {

    public void testParseRangeHeader() {
        final long start = randomLongBetween(0, 10_000);
        final long end = randomLongBetween(start, start + 10_000);
        assertEquals(new HttpHeaderParser.Range(start, end), HttpHeaderParser.parseRangeHeader("bytes=" + start + "-" + end));
    }

    public void testParseRangeHeaderInvalidLong() {
        final BigInteger longOverflow = BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.ONE).add(randomBigInteger());
        assertNull(HttpHeaderParser.parseRangeHeader("bytes=123-" + longOverflow));
        assertNull(HttpHeaderParser.parseRangeHeader("bytes=" + longOverflow + "-123"));
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.http;

import org.elasticsearch.common.Strings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.fixture.HttpHeaderParser;

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

    public void testParseRangeHeaderMultipleRangesNotMatched() {
        assertNull(
            HttpHeaderParser.parseRangeHeader(
                Strings.format(
                    "bytes=%d-%d,%d-%d",
                    randomIntBetween(0, 99),
                    randomIntBetween(100, 199),
                    randomIntBetween(200, 299),
                    randomIntBetween(300, 399)
                )
            )
        );
    }

    public void testParseRangeHeaderEndlessRangeNotMatched() {
        assertNull(HttpHeaderParser.parseRangeHeader(Strings.format("bytes=%d-", randomLongBetween(0, Long.MAX_VALUE))));
    }

    public void testParseRangeHeaderSuffixLengthNotMatched() {
        assertNull(HttpHeaderParser.parseRangeHeader(Strings.format("bytes=-%d", randomLongBetween(0, Long.MAX_VALUE))));
    }
}

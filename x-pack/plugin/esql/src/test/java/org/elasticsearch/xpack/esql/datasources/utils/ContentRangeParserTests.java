/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.utils;

import org.elasticsearch.test.ESTestCase;

public class ContentRangeParserTests extends ESTestCase {

    public void testStandardFormat() {
        assertEquals(Long.valueOf(12345L), ContentRangeParser.parseTotalLength("bytes 0-0/12345"));
    }

    public void testFullRange() {
        assertEquals(Long.valueOf(1000L), ContentRangeParser.parseTotalLength("bytes 0-999/1000"));
    }

    public void testWildcardReturnsNull() {
        assertNull(ContentRangeParser.parseTotalLength("bytes 0-0/*"));
    }

    public void testNullReturnsNull() {
        assertNull(ContentRangeParser.parseTotalLength(null));
    }

    public void testEmptyStringReturnsNull() {
        assertNull(ContentRangeParser.parseTotalLength(""));
    }

    public void testNoSlashReturnsNull() {
        assertNull(ContentRangeParser.parseTotalLength("bytes 0-0"));
    }

    public void testMalformedTotalReturnsNull() {
        assertNull(ContentRangeParser.parseTotalLength("bytes 0-0/abc"));
    }

    public void testLargeObjectSize() {
        assertEquals(Long.valueOf(10_000_000_000L), ContentRangeParser.parseTotalLength("bytes 0-0/10000000000"));
    }

    public void testWhitespaceAroundTotal() {
        assertEquals(Long.valueOf(500L), ContentRangeParser.parseTotalLength("bytes 0-0/ 500 "));
    }
}

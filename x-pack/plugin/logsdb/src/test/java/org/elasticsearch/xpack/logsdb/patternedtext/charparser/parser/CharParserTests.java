/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.patternedtext.charparser.parser;

import org.elasticsearch.test.ESTestCase;

public class CharParserTests extends ESTestCase {

    public void testFindBitmaskForInteger() {
        int[] integerSubTokenBitmaskArrayRanges = { 10, 20, 30, Integer.MAX_VALUE };
        int[] integerSubTokenBitmasks = { 1, 2, 3, 0 };

        assertEquals(1, CharParser.findBitmaskForInteger(-10, integerSubTokenBitmaskArrayRanges, integerSubTokenBitmasks));
        assertEquals(1, CharParser.findBitmaskForInteger(Integer.MIN_VALUE, integerSubTokenBitmaskArrayRanges, integerSubTokenBitmasks));
        assertEquals(1, CharParser.findBitmaskForInteger(0, integerSubTokenBitmaskArrayRanges, integerSubTokenBitmasks));
        assertEquals(1, CharParser.findBitmaskForInteger(9, integerSubTokenBitmaskArrayRanges, integerSubTokenBitmasks));
        assertEquals(1, CharParser.findBitmaskForInteger(10, integerSubTokenBitmaskArrayRanges, integerSubTokenBitmasks));
        assertEquals(2, CharParser.findBitmaskForInteger(11, integerSubTokenBitmaskArrayRanges, integerSubTokenBitmasks));
        assertEquals(2, CharParser.findBitmaskForInteger(19, integerSubTokenBitmaskArrayRanges, integerSubTokenBitmasks));
        assertEquals(2, CharParser.findBitmaskForInteger(20, integerSubTokenBitmaskArrayRanges, integerSubTokenBitmasks));
        assertEquals(3, CharParser.findBitmaskForInteger(21, integerSubTokenBitmaskArrayRanges, integerSubTokenBitmasks));
        assertEquals(3, CharParser.findBitmaskForInteger(29, integerSubTokenBitmaskArrayRanges, integerSubTokenBitmasks));
        assertEquals(3, CharParser.findBitmaskForInteger(30, integerSubTokenBitmaskArrayRanges, integerSubTokenBitmasks));
        assertEquals(0, CharParser.findBitmaskForInteger(31, integerSubTokenBitmaskArrayRanges, integerSubTokenBitmasks));
        assertEquals(0, CharParser.findBitmaskForInteger(Integer.MAX_VALUE, integerSubTokenBitmaskArrayRanges, integerSubTokenBitmasks));
    }
}

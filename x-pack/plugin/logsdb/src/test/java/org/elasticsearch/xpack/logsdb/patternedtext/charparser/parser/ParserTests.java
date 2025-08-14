/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.patternedtext.charparser.parser;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.logsdb.patternedtext.charparser.compiler.SchemaCompiler;
import org.elasticsearch.xpack.logsdb.patternedtext.charparser.patterned.Argument;
import org.elasticsearch.xpack.logsdb.patternedtext.charparser.patterned.PatternedMessage;
import org.elasticsearch.xpack.logsdb.patternedtext.charparser.schema.Schema;

import java.time.format.DateTimeFormatter;
import java.util.Locale;

public class ParserTests extends ESTestCase {

    private static Parser parser;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        parser = new Parser(SchemaCompiler.compile(Schema.getInstance()));
    }

    public void testFindBitmaskForInteger() {
        int[] integerSubTokenBitmaskArrayRanges = { 10, 20, 30, Integer.MAX_VALUE };
        int[] integerSubTokenBitmasks = { 1, 2, 3, 0 };

        assertEquals(1, Parser.findBitmaskForInteger(-10, integerSubTokenBitmaskArrayRanges, integerSubTokenBitmasks));
        assertEquals(1, Parser.findBitmaskForInteger(Integer.MIN_VALUE, integerSubTokenBitmaskArrayRanges, integerSubTokenBitmasks));
        assertEquals(1, Parser.findBitmaskForInteger(0, integerSubTokenBitmaskArrayRanges, integerSubTokenBitmasks));
        assertEquals(1, Parser.findBitmaskForInteger(9, integerSubTokenBitmaskArrayRanges, integerSubTokenBitmasks));
        assertEquals(1, Parser.findBitmaskForInteger(10, integerSubTokenBitmaskArrayRanges, integerSubTokenBitmasks));
        assertEquals(2, Parser.findBitmaskForInteger(11, integerSubTokenBitmaskArrayRanges, integerSubTokenBitmasks));
        assertEquals(2, Parser.findBitmaskForInteger(19, integerSubTokenBitmaskArrayRanges, integerSubTokenBitmasks));
        assertEquals(2, Parser.findBitmaskForInteger(20, integerSubTokenBitmaskArrayRanges, integerSubTokenBitmasks));
        assertEquals(3, Parser.findBitmaskForInteger(21, integerSubTokenBitmaskArrayRanges, integerSubTokenBitmasks));
        assertEquals(3, Parser.findBitmaskForInteger(29, integerSubTokenBitmaskArrayRanges, integerSubTokenBitmasks));
        assertEquals(3, Parser.findBitmaskForInteger(30, integerSubTokenBitmaskArrayRanges, integerSubTokenBitmasks));
        assertEquals(0, Parser.findBitmaskForInteger(31, integerSubTokenBitmaskArrayRanges, integerSubTokenBitmasks));
        assertEquals(0, Parser.findBitmaskForInteger(Integer.MAX_VALUE, integerSubTokenBitmaskArrayRanges, integerSubTokenBitmasks));
    }

    public void testSimpleIpAndNumber() {
        PatternedMessage patternedMessage = parser.parse("Response from 127.0.0.1 took 2000 ms");
        assertEquals("Response from %4 took %I ms", patternedMessage.pattern());
        Argument<?>[] arguments = patternedMessage.arguments();
        assertEquals(2, arguments.length);
        assertEquals("IPV4", arguments[0].type().name());
        assertEquals("INTEGER", arguments[1].type().name());
    }

    public void testTimestampAndIpAndNumber() {
        PatternedMessage patternedMessage = parser.parse("Oct 05 2023 02:48:07 PM INFO Response from 146.10.10.133 took 2000 ms");
        assertEquals("%T INFO Response from %4 took %I ms", patternedMessage.pattern());
        assertEquals(1696517287000L, patternedMessage.timestamp().getTimestamp());
        String pattern = patternedMessage.timestamp().getFormat();
        assertEquals("MMM dd yyyy hh:mm:ss a", pattern);
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern(pattern, Locale.US);
        assertEquals(1696517287000L, TimestampFormat.parseTimestamp(dateTimeFormatter, "Oct 05 2023 02:48:07 PM"));
        Argument<?>[] arguments = patternedMessage.arguments();
        assertEquals(2, arguments.length);
        assertEquals("IPV4", arguments[0].type().name());
        assertEquals("INTEGER", arguments[1].type().name());
    }
}

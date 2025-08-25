/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.patternedtext.charparser.api;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.logsdb.patternedtext.charparser.parser.TimestampFormat;

import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Locale;

import static org.hamcrest.Matchers.instanceOf;

public class ParserTests extends ESTestCase {

    public static final String MESSAGE_WITH_IP_AND_NUMBER = "Response from 127.0.0.1 took 2000 ms";
    public static final String MESSAGE_WITH_TIMESTAMP_IP_AND_NUMBER =
        "Oct 05 2023 02:48:07 PM INFO Response from 146.10.10.133 took 2000 ms";
    private static Parser parser;
    private static StringBuilder patternedMessage;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        parser = ParserFactory.createParser();
        patternedMessage = new StringBuilder();
    }

    public void testSimpleIpAndNumber() throws ParseException {
        List<Argument<?>> parsedArguments = parser.parse(MESSAGE_WITH_IP_AND_NUMBER);
        Parser.constructPattern(MESSAGE_WITH_IP_AND_NUMBER, parsedArguments, patternedMessage, true);
        assertEquals("Response from %4 took %I ms", patternedMessage.toString());
        assertEquals(2, parsedArguments.size());
        assertEquals("IPV4", parsedArguments.getFirst().type().name());
        assertEquals("INTEGER", parsedArguments.get(1).type().name());
    }

    public void testTimestampAndIpAndNumber() throws ParseException {
        // todo - there is a bug here, "Oct 05 2023 02:48:07 PM" SHOULD NOT match the timestamp format and "Oct, 05 2023 02:48:07 PM" SHOULD
        List<Argument<?>> parsedArguments = parser.parse(MESSAGE_WITH_TIMESTAMP_IP_AND_NUMBER);
        Parser.constructPattern(MESSAGE_WITH_TIMESTAMP_IP_AND_NUMBER, parsedArguments, patternedMessage, true);
        assertEquals("%T INFO Response from %4 took %I ms", patternedMessage.toString());
        assertEquals(3, parsedArguments.size());
        assertThat(parsedArguments.getFirst(), instanceOf(Timestamp.class));
        Timestamp timestamp = (Timestamp) parsedArguments.getFirst();
        assertEquals(1696517287000L, timestamp.getTimestampMillis());
        String pattern = timestamp.getFormat();
        assertEquals("MMM, dd yyyy hh:mm:ss a", pattern);
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern(pattern, Locale.US);
        assertEquals(1696517287000L, TimestampFormat.parseTimestamp(dateTimeFormatter, "Oct, 05 2023 02:48:07 PM"));
        assertEquals("IPV4", parsedArguments.get(1).type().name());
        assertEquals("INTEGER", parsedArguments.get(2).type().name());
    }
}

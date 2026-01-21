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

    private static Parser parser;
    private static StringBuilder patternedMessage;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        parser = ParserFactory.createParser();
        patternedMessage = new StringBuilder();
    }

    public void testSimpleIpAndNumber() throws ParseException {
        String messageWithIpAndNumber = "Response from 127.0.0.1 took 2000 ms";
        List<Argument<?>> parsedArguments = parser.parse(messageWithIpAndNumber);
        Parser.constructPattern(messageWithIpAndNumber, parsedArguments, patternedMessage, true);
        assertEquals("Response from %4 took %I ms", patternedMessage.toString());
        assertEquals(2, parsedArguments.size());
        assertEquals("IPV4", parsedArguments.getFirst().type().name());
        Argument<?> argument = parsedArguments.get(1);
        assertThat(argument, instanceOf(IntegerArgument.class));
        assertEquals(2000, ((IntegerArgument) argument).value().intValue());
        assertNull("Sign should be null", ((IntegerArgument) argument).sign());
    }

    public void testRFC1123TimestampAndIpAndNumber() throws ParseException {
        String messageWithTimestampIpAndNumber = "Oct, 05 2023 02:48:07 PM INFO Response from 146.10.10.133 took 2000 ms";
        List<Argument<?>> parsedArguments = parser.parse(messageWithTimestampIpAndNumber);
        Parser.constructPattern(messageWithTimestampIpAndNumber, parsedArguments, patternedMessage, true);
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

    public void testInvalidTimestamp() throws ParseException {
        String message = "Oct 05 2023 02:48:07 PM INFO Response from 146.10.10.133 took 2000 ms";
        List<Argument<?>> parsedArguments = parser.parse(message);
        Parser.constructPattern(message, parsedArguments, patternedMessage, true);
        // todo - add support for local time - based on java.time.LocalTime
        assertEquals("Oct %I %I %I:%I:%I PM INFO Response from %4 took %I ms", patternedMessage.toString());
    }

    public void testNumberArgumentsWithSign() throws ParseException {
        String message = "-5 is negative, this:+10:-8 is both and this is positive: +20";
        List<Argument<?>> parsedArguments = parser.parse(message);
        Parser.constructPattern(message, parsedArguments, patternedMessage, true);
        assertEquals("%I is negative, this:%I:%I is both and this is positive: %I", patternedMessage.toString());
        assertEquals(4, parsedArguments.size());
        Argument<?> argument = parsedArguments.getFirst();
        assertThat(argument, instanceOf(IntegerArgument.class));
        assertEquals(Sign.MINUS, ((IntegerArgument) argument).sign());
        assertEquals(-5, ((IntegerArgument) argument).value().intValue());
        argument = parsedArguments.get(1);
        assertThat(argument, instanceOf(IntegerArgument.class));
        assertEquals(Sign.PLUS, ((IntegerArgument) argument).sign());
        assertEquals(10, ((IntegerArgument) argument).value().intValue());
        argument = parsedArguments.get(2);
        assertThat(argument, instanceOf(IntegerArgument.class));
        assertEquals(Sign.MINUS, ((IntegerArgument) argument).sign());
        assertEquals(-8, ((IntegerArgument) argument).value().intValue());
        argument = parsedArguments.get(3);
        assertThat(argument, instanceOf(IntegerArgument.class));
        assertEquals(Sign.PLUS, ((IntegerArgument) argument).sign());
        assertEquals(20, ((IntegerArgument) argument).value().intValue());
    }

    public void testFloatingPointArguments() throws ParseException {
        String message = "-5.08 is at the beginning, and here is one at the end: -1.09e-2";
        List<Argument<?>> parsedArguments = parser.parse(message);
        Parser.constructPattern(message, parsedArguments, patternedMessage, true);
        assertEquals("%F is at the beginning, and here is one at the end: %F", patternedMessage.toString());
        assertEquals(2, parsedArguments.size());
        Argument<?> argument = parsedArguments.getFirst();
        assertThat(argument, instanceOf(DoubleArgument.class));
        assertEquals(-5.08, ((DoubleArgument) argument).value(), 0);
        argument = parsedArguments.get(1);
        assertThat(argument, instanceOf(DoubleArgument.class));
        assertEquals(-0.0109, ((DoubleArgument) argument).value(), 0);
    }

    public void testBigIntegerArgument() throws ParseException {
        String message = "The value is 123456789 in the message";
        List<Argument<?>> parsedArguments = parser.parse(message);
        Parser.constructPattern(message, parsedArguments, patternedMessage, true);
        assertEquals("The value is %I in the message", patternedMessage.toString());
        assertEquals(1, parsedArguments.size());
        Argument<?> argument = parsedArguments.getFirst();
        assertThat(argument, instanceOf(IntegerArgument.class));
        assertEquals(123456789, ((IntegerArgument) argument).value().intValue());
    }

    public void testApacheLogTimestamp() throws ParseException {
        String message = "05/Oct/2023:14:48:00 +0000 GET /index.html 200";
        List<Argument<?>> parsedArguments = parser.parse(message);
        Parser.constructPattern(message, parsedArguments, patternedMessage, true);
        assertEquals("%T GET /index.html %I", patternedMessage.toString());
        assertEquals(2, parsedArguments.size());
        Argument<?> argument = parsedArguments.getFirst();
        assertThat(argument, instanceOf(Timestamp.class));
        Timestamp timestamp = (Timestamp) argument;
        assertEquals(1696517280000L, timestamp.getTimestampMillis());
        String pattern = timestamp.getFormat();
        assertEquals("dd/MMM/yyyy:HH:mm:ss Z", pattern);
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern(pattern, Locale.US);
        assertEquals(1696517280000L, TimestampFormat.parseTimestamp(dateTimeFormatter, "05/Oct/2023:14:48:00 +0000"));
        argument = parsedArguments.get(1);
        assertThat(argument, instanceOf(IntegerArgument.class));
        assertEquals(200, ((IntegerArgument) argument).value().intValue());
    }

    public void testApacheErrorLogTimestamp() throws ParseException {
        String message = "[Thu Oct 05 14:48:00.123 2023] [info] [pid 9] core.c(4739): [client 172.17.0.1:50764] AH00128: File does not "
            + "exist: /usr/local/apache2/htdocs/favicon.ico.";
        // todo - timestamp with NA component (day of week) not yet supported as well as IP4V address
    }
}

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

    public void testUuidStandard() throws ParseException {
        String message = "request 123e4567-e89b-12d3-a456-426614174000 completed";
        List<Argument<?>> parsedArguments = parser.parse(message);
        Parser.constructPattern(message, parsedArguments, patternedMessage, true);
        assertEquals("request %U completed", patternedMessage.toString());
        assertEquals(1, parsedArguments.size());
        assertThat(parsedArguments.getFirst(), instanceOf(UUIDArgument.class));
        assertEquals("UUID", parsedArguments.getFirst().type().name());
    }

    public void testUuidAllGroupsMixed() throws ParseException {
        // every group has BOTH a letter and a digit, so none hits the all-digit integer branch
        String message = "request 1a2b3c4d-1a2b-3c4d-5e6f-1a2b3c4d5e6f completed";
        List<Argument<?>> parsedArguments = parser.parse(message);
        Parser.constructPattern(message, parsedArguments, patternedMessage, true);
        assertEquals("request %U completed", patternedMessage.toString());
    }

    public void testUuidLeadingAllDigitGroup() throws ParseException {
        // first group is all digits (hits the integer branch at position 0)
        String message = "id 12345678-e89b-12d3-a456-426614174000 ok";
        List<Argument<?>> parsedArguments = parser.parse(message);
        Parser.constructPattern(message, parsedArguments, patternedMessage, true);
        assertEquals("id %U ok", patternedMessage.toString());
        assertThat(parsedArguments.getFirst(), instanceOf(UUIDArgument.class));
    }

    public void testUuidCompact() throws ParseException {
        String message = "token 123e4567e89b12d3a456426614174000 accepted";
        List<Argument<?>> parsedArguments = parser.parse(message);
        Parser.constructPattern(message, parsedArguments, patternedMessage, true);
        assertEquals("token %U accepted", patternedMessage.toString());
        assertThat(parsedArguments.getFirst(), instanceOf(UUIDArgument.class));
    }

    public void testUuidWrongGroupLengthNotMatched() throws ParseException {
        // second group is 3 hex chars instead of 4 -> must NOT be recognized as a UUID (length matters)
        String message = "id 12345678-e89-12d3-a456-426614174000 x";
        List<Argument<?>> parsedArguments = parser.parse(message);
        Parser.constructPattern(message, parsedArguments, patternedMessage, true);
        assertNotEquals("id %U x", patternedMessage.toString());
    }

    public void testSparkTimestamp() throws ParseException {
        // Spark log format: yy/MM/dd HH:mm:ss  (2-digit year interpreted as 20yy)
        String message = "17/06/09 20:10:40 INFO Executor task 42 finished";
        List<Argument<?>> parsedArguments = parser.parse(message);
        Parser.constructPattern(message, parsedArguments, patternedMessage, true);
        assertEquals("%T INFO Executor task %I finished", patternedMessage.toString());
        assertThat(parsedArguments.getFirst(), instanceOf(Timestamp.class));
        Timestamp timestamp = (Timestamp) parsedArguments.getFirst();
        assertEquals(1497039040000L, timestamp.getTimestampMillis()); // 2017-06-09T20:10:40Z
    }

    public void testZookeeperTimestamp() throws ParseException {
        // Zookeeper log format: YYYY-MM-DD HH:mm:ss,SSS  (comma before millis)
        String message = "2015-07-29 17:41:41,313 INFO server started";
        List<Argument<?>> parsedArguments = parser.parse(message);
        Parser.constructPattern(message, parsedArguments, patternedMessage, true);
        assertEquals("%T INFO server started", patternedMessage.toString());
        assertThat(parsedArguments.getFirst(), instanceOf(Timestamp.class));
        Timestamp timestamp = (Timestamp) parsedArguments.getFirst();
        assertEquals(1438191701313L, timestamp.getTimestampMillis()); // 2015-07-29T17:41:41.313Z
    }

    public void testHdfsTimestamp() throws ParseException {
        // HDFS log format: yymmdd HHMMSS (compact, no separators within date or time)
        String message = "081109 203615 148 starting";
        List<Argument<?>> parsedArguments = parser.parse(message);
        Parser.constructPattern(message, parsedArguments, patternedMessage, true);
        assertEquals("%T %I starting", patternedMessage.toString());
        assertThat(parsedArguments.getFirst(), instanceOf(Timestamp.class));
        Timestamp timestamp = (Timestamp) parsedArguments.getFirst();
        assertEquals(1226262975000L, timestamp.getTimestampMillis()); // 2008-11-09T20:36:15Z
    }

    public void testHdfsCompactOutOfRangeIsNotTimestamp() throws ParseException {
        // both 6-digit numbers are out of the yymmdd/HHMMSS ranges -> rejected by the constraint, emitted as plain integers
        String message = "999999 999999 done";
        List<Argument<?>> parsedArguments = parser.parse(message);
        Parser.constructPattern(message, parsedArguments, patternedMessage, true);
        assertEquals("%I %I done", patternedMessage.toString());
    }

    public void testHdfsCompactInRangeButInvalidIsNotTimestamp() throws ParseException {
        // 001345 is in the [101,991231] range but decomposes to month 13 / day 45 -> NOT a valid date.
        // Must not throw, and must fall back to plain integers rather than a timestamp.
        String message = "001345 203615 done";
        List<Argument<?>> parsedArguments = parser.parse(message);
        Parser.constructPattern(message, parsedArguments, patternedMessage, true);
        assertEquals("%I %I done", patternedMessage.toString());
    }

    public void testHdfsCompactInvalidTimeIsNotTimestamp() throws ParseException {
        // 081109 is a valid date but 209999 decomposes to minute 99 / second 99 -> invalid time -> fall back to plain integers
        String message = "081109 209999 done";
        List<Argument<?>> parsedArguments = parser.parse(message);
        Parser.constructPattern(message, parsedArguments, patternedMessage, true);
        assertEquals("%I %I done", patternedMessage.toString());
    }

    public void testSparkTimestampLeadingZeroYear() throws ParseException {
        // 2008 -> yy=08 (a leading-zero 2-digit year). Must be recognized as %T (currently exposes the value-based {2} bug).
        String message = "08/06/09 20:10:40 INFO done";
        List<Argument<?>> parsedArguments = parser.parse(message);
        Parser.constructPattern(message, parsedArguments, patternedMessage, true);
        assertEquals("%T INFO done", patternedMessage.toString());
    }

    public void testHdfsCompactRequiresExactlySixDigits() throws ParseException {
        // "1109" is only 4 digits; it decomposes to a valid-looking date (2000-11-09) but a compact date MUST be
        // exactly 6 digits (2 per component), so it must NOT be treated as a timestamp.
        String message = "1109 203615 x";
        List<Argument<?>> parsedArguments = parser.parse(message);
        Parser.constructPattern(message, parsedArguments, patternedMessage, true);
        assertEquals("%I %I x", patternedMessage.toString());
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

    public void testLoggingLibrariesDatetimeTimestamp() throws ParseException {
        // ISO-8601-like: an uppercase 'T' separates the date and time. 'T' is an ordinary content character, but within this timestamp
        // token it acts as a subToken delimiter (declared via special_sub_token_delimiters). It must NOT corrupt the preceding YYYY.
        String message = "05/Oct/2023T14:48:00 +0000 GET /index.html 200";
        List<Argument<?>> parsedArguments = parser.parse(message);
        Parser.constructPattern(message, parsedArguments, patternedMessage, true);
        assertEquals("%T GET /index.html %I", patternedMessage.toString());
        assertEquals(2, parsedArguments.size());
        assertThat(parsedArguments.getFirst(), instanceOf(Timestamp.class));
        Timestamp timestamp = (Timestamp) parsedArguments.getFirst();
        assertEquals(1696517280000L, timestamp.getTimestampMillis()); // 2023-10-05T14:48:00Z
        assertThat(parsedArguments.get(1), instanceOf(IntegerArgument.class));
        assertEquals(200, ((IntegerArgument) parsedArguments.get(1)).value().intValue());
    }

    public void testLoggingLibrariesDatetimeTimestampDifferentDate() throws ParseException {
        // a different date/time (with all-different digits and a boundary-crossing time) to confirm the 'T' split is not date-specific.
        // Note: timezone offset is not yet applied by the parser (see TimestampFormat "todo - properly compute timezone offset"), so we
        // use +0000 to keep the assertion robust to that pending work.
        String message = "31/Dec/2020T23:59:59 +0000 end";
        List<Argument<?>> parsedArguments = parser.parse(message);
        Parser.constructPattern(message, parsedArguments, patternedMessage, true);
        assertEquals("%T end", patternedMessage.toString());
        assertThat(parsedArguments.getFirst(), instanceOf(Timestamp.class));
        Timestamp timestamp = (Timestamp) parsedArguments.getFirst();
        assertEquals(1609459199000L, timestamp.getTimestampMillis()); // 2020-12-31T23:59:59Z
    }

    public void testUppercaseTIsContentOutsideTimestamp() throws ParseException {
        // uppercase 'T' inside ordinary words (both leading and interior) must remain content, never a subToken delimiter
        String message = "TRACE STARTING TASK 5";
        List<Argument<?>> parsedArguments = parser.parse(message);
        Parser.constructPattern(message, parsedArguments, patternedMessage, true);
        assertEquals("TRACE STARTING TASK %I", patternedMessage.toString());
        assertEquals(1, parsedArguments.size());
        assertThat(parsedArguments.getFirst(), instanceOf(IntegerArgument.class));
    }

    public void testDateLikePrefixWithInvalidTimeAfterTIsNotTimestamp() throws ParseException {
        // 'T' may speculatively act as a delimiter after a valid-looking DD/Mon/YYYY, but if what follows is not a valid time the whole
        // timestamp must fail: crucially, NO %T (bogus/partial timestamp) is produced. The token then falls back to its generic parse,
        // where the numeric subTokens surface as plain integers and the 'T' delimiter stays literal.
        String message = "05/Oct/2023TASK done";
        List<Argument<?>> parsedArguments = parser.parse(message);
        Parser.constructPattern(message, parsedArguments, patternedMessage, true);
        assertEquals("%I/Oct/%ITASK done", patternedMessage.toString());
        for (Argument<?> argument : parsedArguments) {
            assertThat(argument, instanceOf(IntegerArgument.class)); // no Timestamp argument was produced
        }
    }

    public void testNegativeTwoDigitYearIsNotTimestamp() throws ParseException {
        // yy {2} is unsigned; a sign-prefixed "-7" must not match it (the compiler floors %I value ranges at 0), so a Spark-shaped line
        // with a negative year must NOT be recognized as a timestamp. This guards the value-neutral {n} + base-type-floor design.
        String message = "-7/06/09 20:10:40 INFO done";
        List<Argument<?>> parsedArguments = parser.parse(message);
        Parser.constructPattern(message, parsedArguments, patternedMessage, true);
        boolean hasTimestamp = false;
        for (Argument<?> argument : parsedArguments) {
            if (argument instanceof Timestamp) {
                hasTimestamp = true;
            }
        }
        assertFalse("a negative two-digit year must not form a timestamp", hasTimestamp);
    }
}

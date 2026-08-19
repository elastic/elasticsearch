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

    public void testQuoteAndParenWrappedInteriorNumber() throws ParseException {
        // consecutive interior boundary chars of DIFFERENT kinds (paren + double-quote) wrapping a value
        String message = "event(\"500\") ok";
        List<Argument<?>> parsedArguments = parser.parse(message);
        Parser.constructPattern(message, parsedArguments, patternedMessage, true);
        assertEquals("event(\"%I\") ok", patternedMessage.toString());
    }

    public void testSingleQuoteAndParenWrappedInteriorNumber() throws ParseException {
        String message = "event('500') ok";
        List<Argument<?>> parsedArguments = parser.parse(message);
        Parser.constructPattern(message, parsedArguments, patternedMessage, true);
        assertEquals("event('%I') ok", patternedMessage.toString());
    }

    public void testEmptyInteriorBracketRunStaysLiteral() throws ParseException {
        // an empty interior boundary run ("[]") with content after it splits on the run but extracts nothing from it;
        // "sshd[]" (no content after the run) is a plain trailing suffix and is untouched
        String message = "foo[]bar 9";
        List<Argument<?>> parsedArguments = parser.parse(message);
        Parser.constructPattern(message, parsedArguments, patternedMessage, true);
        assertEquals("foo[]bar %I", patternedMessage.toString());
    }

    public void testEmptySubtoken() throws ParseException {
        String message = "foo/ 9";
        List<Argument<?>> parsedArguments = parser.parse(message);
        Parser.constructPattern(message, parsedArguments, patternedMessage, true);
        assertEquals("foo/ %I", patternedMessage.toString());
        message = "foo// 9";
        parsedArguments = parser.parse(message);
        Parser.constructPattern(message, parsedArguments, patternedMessage, true);
        assertEquals("foo// %I", patternedMessage.toString());
        message = "/ 9";
        parsedArguments = parser.parse(message);
        Parser.constructPattern(message, parsedArguments, patternedMessage, true);
        assertEquals("/ %I", patternedMessage.toString());
        message = "// 9";
        parsedArguments = parser.parse(message);
        Parser.constructPattern(message, parsedArguments, patternedMessage, true);
        assertEquals("// %I", patternedMessage.toString());
        message = "foo--bar 9";
        parsedArguments = parser.parse(message);
        Parser.constructPattern(message, parsedArguments, patternedMessage, true);
        assertEquals("foo--bar %I", patternedMessage.toString());
        message = "--foo--bar 9";
        parsedArguments = parser.parse(message);
        Parser.constructPattern(message, parsedArguments, patternedMessage, true);
        assertEquals("--foo--bar %I", patternedMessage.toString());
    }

    public void testMultipleInteriorGroupsEachExtracted() throws ParseException {
        // several interior groups in one token each split and extract independently
        String message = "req(1)id(2)seq(3)";
        List<Argument<?>> parsedArguments = parser.parse(message);
        Parser.constructPattern(message, parsedArguments, patternedMessage, true);
        assertEquals("req(%I)id(%I)seq(%I)", patternedMessage.toString());
    }

    // A token that CONTAINS digits but matches no specific type (its bitmask is zeroed by mixing digits with letters or with a character
    // outside every sub-token charset) must fall back to a keyword placeholder (%A), never leak into the template as a literal.

    public void testMixedAlphanumericTokenBecomesKeyword() throws ParseException {
        // digits mixed with non-hex letters -> bitmask 0, but it contains digits, so it collapses to %A instead of staying literal
        // (this is the real OpenSSH "Invalid user test9 from ..." case; note an all-hex token like "abc123" is legitimately typed %H)
        String message = "user test9 in";
        List<Argument<?>> parsedArguments = parser.parse(message);
        Parser.constructPattern(message, parsedArguments, patternedMessage, true);
        assertEquals("user %A in", patternedMessage.toString());
    }

    public void testHashSeparatedNumericBlobBecomesKeyword() throws ParseException {
        // '#' is in no sub-token charset (zeroes the bitmask), but the blob contains digits -> one %A (this is the HealthApp
        // "getTodayTotalDetailSteps = 1514038440000##7007##..." family that otherwise explodes to one template per value)
        String message = "steps 100##200##300 end";
        List<Argument<?>> parsedArguments = parser.parse(message);
        Parser.constructPattern(message, parsedArguments, patternedMessage, true);
        assertEquals("steps %A end", patternedMessage.toString());
    }

    public void testDigitBearingSubTokenBecomesKeywordEvenWhenNotLastInToken() throws ParseException {
        // per-sub-token digit tracking: the digit-bearing sub-token is NOT the last one in its token, so a global
        // isCurSubTokenContainsDigits flag (state of the LAST sub-token) would miss it. "xy12" (non-hex) -> %A, "zt" stays literal.
        String message = "x xy12.zt y";
        List<Argument<?>> parsedArguments = parser.parse(message);
        Parser.constructPattern(message, parsedArguments, patternedMessage, true);
        assertEquals("x %A.zt y", patternedMessage.toString());
    }

    public void testNonDigitUnknownTokenStaysLiteral() throws ParseException {
        // guard: a token with an out-of-charset character but NO digits stays literal (we only promote digit-bearing tokens)
        String message = "path a#b end";
        List<Argument<?>> parsedArguments = parser.parse(message);
        Parser.constructPattern(message, parsedArguments, patternedMessage, true);
        assertEquals("path a#b end", patternedMessage.toString());
    }

    public void testEmbeddedMidLineTimestampCollapses() throws ParseException {
        String message = "connection at Jun 14 15:16:01 done";
        List<Argument<?>> parsedArguments = parser.parse(message);
        Parser.constructPattern(message, parsedArguments, patternedMessage, true);
        assertEquals("connection at %T done", patternedMessage.toString());
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
        // Spark log format: yy/MM/dd HH:mm:ss (2-digit year interpreted as 20yy)
        String message = "17/06/09 20:10:40 INFO Executor task 42 finished";
        List<Argument<?>> parsedArguments = parser.parse(message);
        Parser.constructPattern(message, parsedArguments, patternedMessage, true);
        assertEquals("%T INFO Executor task %I finished", patternedMessage.toString());
        assertThat(parsedArguments.getFirst(), instanceOf(Timestamp.class));
        Timestamp timestamp = (Timestamp) parsedArguments.getFirst();
        assertEquals(1497039040000L, timestamp.getTimestampMillis()); // 2017-06-09T20:10:40Z
    }

    public void testZookeeperTimestamp() throws ParseException {
        // Zookeeper log format: YYYY-MM-DD HH:mm:ss,SSS (comma before millis)
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

    // ---- BSD / RFC-3164 syslog timestamps: "MMM dd HH:mm:ss" with NO year (e.g. Linux, OpenSSH) ----
    // The month name ($Mon) anchors it; the year is absent, so it defaults to a fixed leap year (2000) so
    // that any valid month/day (incl. Feb 29) yields a valid calendar date. Only the %T collapse matters
    // for templates; the defaulted-year value is asserted to pin the documented behavior.

    public void testSyslogBsdTimestamp() throws ParseException {
        String message = "Jun 14 15:16:01 host service started";
        List<Argument<?>> parsedArguments = parser.parse(message);
        Parser.constructPattern(message, parsedArguments, patternedMessage, true);
        assertEquals("%T host service started", patternedMessage.toString());
        assertThat(parsedArguments.getFirst(), instanceOf(Timestamp.class));
        Timestamp timestamp = (Timestamp) parsedArguments.getFirst();
        assertEquals(960995761000L, timestamp.getTimestampMillis()); // 2000-06-14T15:16:01Z (default year)
    }

    public void testSyslogBsdTimestampOpenSsh() throws ParseException {
        String message = "Dec 10 06:55:46 LabSZ sshd connection";
        List<Argument<?>> parsedArguments = parser.parse(message);
        Parser.constructPattern(message, parsedArguments, patternedMessage, true);
        assertEquals("%T LabSZ sshd connection", patternedMessage.toString());
        assertThat(parsedArguments.getFirst(), instanceOf(Timestamp.class));
        assertEquals(976431346000L, ((Timestamp) parsedArguments.getFirst()).getTimestampMillis()); // 2000-12-10T06:55:46Z
    }

    public void testSyslogBsdTimestampMidnightBoundary() throws ParseException {
        String message = "Jan 01 00:00:00 x";
        List<Argument<?>> parsedArguments = parser.parse(message);
        Parser.constructPattern(message, parsedArguments, patternedMessage, true);
        assertEquals("%T x", patternedMessage.toString());
        assertEquals(946684800000L, ((Timestamp) parsedArguments.getFirst()).getTimestampMillis()); // 2000-01-01T00:00:00Z
    }

    public void testSyslogBsdPaddedDayTimestamp() throws ParseException {
        // BSD/RFC-3164 space-pads single-digit days to width 2, so a single-digit day has TWO spaces before it
        // ("Jul 1"). The extra space produces an empty token that must be absorbed so it still collapses to %T,
        // exactly like the two-digit-day form "Jun 14". The %T must span the actual padded text (both spaces).
        String message = "Jul  1 00:21:28 combo x";
        List<Argument<?>> parsedArguments = parser.parse(message);
        Parser.constructPattern(message, parsedArguments, patternedMessage, true);
        assertEquals("%T combo x", patternedMessage.toString());
        assertThat(parsedArguments.getFirst(), instanceOf(Timestamp.class));
        assertEquals(962410888000L, ((Timestamp) parsedArguments.getFirst()).getTimestampMillis()); // 2000-07-01T00:21:28Z
    }

    public void testAsctimeDateTimeTimestamp() throws ParseException {
        String message = "Fri Jun 14 15:16:01 2005 x";
        List<Argument<?>> parsedArguments = parser.parse(message);
        Parser.constructPattern(message, parsedArguments, patternedMessage, true);
        assertEquals("%T x", patternedMessage.toString());
        assertThat(parsedArguments.getFirst(), instanceOf(Timestamp.class));
        assertEquals(1118762161000L, ((Timestamp) parsedArguments.getFirst()).getTimestampMillis()); // 2005-06-14T15:16:01Z
    }

    public void testSyslogBsdTimestampLeapDay() throws ParseException {
        // Feb 29 is only a valid date because the defaulted year (2000) is a leap year - guards against a
        // non-leap default that would throw DateTimeException and silently drop the timestamp.
        String message = "Feb 29 12:00:00 leap";
        List<Argument<?>> parsedArguments = parser.parse(message);
        Parser.constructPattern(message, parsedArguments, patternedMessage, true);
        assertEquals("%T leap", patternedMessage.toString());
        assertEquals(951825600000L, ((Timestamp) parsedArguments.getFirst()).getTimestampMillis()); // 2000-02-29T12:00:00Z
    }

    public void testSyslogBsdNoTimeIsNotTimestamp() throws ParseException {
        // month + day but no HH:mm:ss -> not a complete timestamp -> must not collapse to %T
        String message = "Jun 14 combo";
        List<Argument<?>> parsedArguments = parser.parse(message);
        Parser.constructPattern(message, parsedArguments, patternedMessage, true);
        assertEquals("Jun %I combo", patternedMessage.toString());
    }

    public void testSyslogBsdInvalidDayIsNotTimestamp() throws ParseException {
        // day 32 is out of the [1,31] range -> $DD does not match -> not a timestamp
        String message = "Jun 32 15:16:01 x";
        List<Argument<?>> parsedArguments = parser.parse(message);
        Parser.constructPattern(message, parsedArguments, patternedMessage, true);
        // Not a timestamp (day 32). "Jun" has no digits so it stays literal (consistent with testSyslogBsdNoTimeIsNotTimestamp);
        // previously it rendered %A due to the shared isCurSubTokenContainsDigits flag bleeding a sibling sub-token's digits onto it.
        assertEquals("Jun %I %I:%I:%I x", patternedMessage.toString());
    }

    public void testSyslogBsdInvalidHourIsNotTimestamp() throws ParseException {
        // hour 25 is out of the [0,23] range -> $timeS does not match -> not a timestamp
        String message = "Jun 14 25:16:01 x";
        List<Argument<?>> parsedArguments = parser.parse(message);
        Parser.constructPattern(message, parsedArguments, patternedMessage, true);
        assertEquals("Jun %I %I:%I:%I x", patternedMessage.toString());
    }

    // ---- Proxifier timestamps: "[MM.DD HH:mm:ss]" (no year; the [] are boundary chars, stripped). ----
    // The MM.DD date fragment is a %F token, so a decimal that is NOT part of a full Proxifier timestamp still
    // renders as a double (never literal), avoiding any regression for ordinary "N.M" decimals.

    public void testProxifierTimestamp() throws ParseException {
        String message = "[10.30 16:49:06] chrome started";
        List<Argument<?>> parsedArguments = parser.parse(message);
        Parser.constructPattern(message, parsedArguments, patternedMessage, true);
        assertEquals("[%T] chrome started", patternedMessage.toString());
        assertThat(parsedArguments.getFirst(), instanceOf(Timestamp.class));
        assertEquals(972924546000L, ((Timestamp) parsedArguments.getFirst()).getTimestampMillis()); // 2000-10-30T16:49:06Z
    }

    public void testProxifierTimestampBoundary() throws ParseException {
        String message = "[01.01 00:00:00] x";
        List<Argument<?>> parsedArguments = parser.parse(message);
        Parser.constructPattern(message, parsedArguments, patternedMessage, true);
        assertEquals("[%T] x", patternedMessage.toString());
        assertEquals(946684800000L, ((Timestamp) parsedArguments.getFirst()).getTimestampMillis()); // 2000-01-01T00:00:00Z
    }

    public void testProxifierStandaloneDecimalStaysDouble() throws ParseException {
        // a decimal that looks like MM.DD but is NOT followed by a time must remain a %F double (regression guard)
        String message = "version 10.30 released";
        List<Argument<?>> parsedArguments = parser.parse(message);
        Parser.constructPattern(message, parsedArguments, patternedMessage, true);
        assertEquals("version %F released", patternedMessage.toString());
        assertThat(parsedArguments.getFirst(), instanceOf(DoubleArgument.class));
    }

    public void testProxifierBracketedDecimalWithoutTimeStaysDouble() throws ParseException {
        // bracketed decimal with no following time -> not a Proxifier timestamp -> stays %F, not %T
        String message = "[10.30] done";
        List<Argument<?>> parsedArguments = parser.parse(message);
        Parser.constructPattern(message, parsedArguments, patternedMessage, true);
        assertEquals("[%F] done", patternedMessage.toString());
    }

    public void testProxifierInvalidMonthIsNotTimestamp() throws ParseException {
        // 13 is out of the [1,12] month range -> $MM does not match -> not a Proxifier timestamp
        String message = "[13.30 16:49:06] x";
        List<Argument<?>> parsedArguments = parser.parse(message);
        Parser.constructPattern(message, parsedArguments, patternedMessage, true);
        assertEquals("[%F %I:%I:%I] x", patternedMessage.toString());
    }

    public void testProxifierInvalidHourIsNotTimestamp() throws ParseException {
        // 25 is out of the [0,23] hour range -> $timeS does not match -> not a Proxifier timestamp
        String message = "[10.30 25:49:06] x";
        List<Argument<?>> parsedArguments = parser.parse(message);
        Parser.constructPattern(message, parsedArguments, patternedMessage, true);
        assertEquals("[%F %I:%I:%I] x", patternedMessage.toString());
    }

    // ---- HealthApp timestamps: "yyyyMMdd-HH:mm:ss:SSS" (8-digit compact date, colon before millis), followed
    // by '|'-delimited fields. Requires '|' to be a token delimiter so the timestamp is its own token. ----

    public void testHealthAppTimestamp() throws ParseException {
        String message = "20171223-22:15:29:606|Step_LSC|30002312|flush";
        List<Argument<?>> parsedArguments = parser.parse(message);
        Parser.constructPattern(message, parsedArguments, patternedMessage, true);
        assertEquals("%T|Step_LSC|%I|flush", patternedMessage.toString());
        assertThat(parsedArguments.getFirst(), instanceOf(Timestamp.class));
        assertEquals(1514067329606L, ((Timestamp) parsedArguments.getFirst()).getTimestampMillis()); // 2017-12-23T22:15:29.606Z
    }

    public void testHealthAppTimestampBoundary() throws ParseException {
        String message = "20000101-00:00:00:000|x";
        List<Argument<?>> parsedArguments = parser.parse(message);
        Parser.constructPattern(message, parsedArguments, patternedMessage, true);
        assertEquals("%T|x", patternedMessage.toString());
        assertEquals(946684800000L, ((Timestamp) parsedArguments.getFirst()).getTimestampMillis()); // 2000-01-01T00:00:00.000Z
    }

    public void testHealthAppOutOfRangeDateIsNotTimestamp() throws ParseException {
        // 12345678 is below the [20000101,21001231] range -> $yyyymmdd does not match -> not a timestamp
        String message = "12345678-22:15:29:606|x";
        List<Argument<?>> parsedArguments = parser.parse(message);
        Parser.constructPattern(message, parsedArguments, patternedMessage, true);
        assertEquals("%I-%I:%I:%I:%I|x", patternedMessage.toString());
    }

    public void testHealthAppInvalidDateIsNotTimestamp() throws ParseException {
        // 20171332 is in range but decomposes to month 13 / day 32 -> DateTimeException -> must fall back, not %T
        String message = "20171332-22:15:29:606|x";
        List<Argument<?>> parsedArguments = parser.parse(message);
        Parser.constructPattern(message, parsedArguments, patternedMessage, true);
        // invalid date (month 13/day 32) -> DateTimeException fallback -> sub-tokens emitted individually (no %T)
        assertEquals("%I-%I:%I:%I:%I|x", patternedMessage.toString());
    }

    public void testPipeIsTokenDelimiter() throws ParseException {
        // '|' becomes a token delimiter (needed to isolate the HealthApp timestamp); an embedded integer field collapses.
        // (Uses non-hex letters x/y - a single 'a'/'b' would be recognized as a %H hex value.)
        String message = "x|123|y";
        List<Argument<?>> parsedArguments = parser.parse(message);
        Parser.constructPattern(message, parsedArguments, patternedMessage, true);
        assertEquals("x|%I|y", patternedMessage.toString());
    }

    // ---- Grouping brackets () [] {} are sub-token delimiters: a value wrapped by brackets is extracted even
    // when the bracket is INTERIOR (content before it), e.g. a process id. Empty sub-tokens (from a bracket at
    // a token edge) are skipped, so bracket-wrapped typed values (IPv4, timestamps) are still recognized. ----

    public void testInteriorBracketExtractsNumber() throws ParseException {
        String message = "sshd[24200] authentication";
        List<Argument<?>> parsedArguments = parser.parse(message);
        Parser.constructPattern(message, parsedArguments, patternedMessage, true);
        assertEquals("sshd[%I] authentication", patternedMessage.toString());
    }

    public void testInteriorParenAndBracketExtractNumber() throws ParseException {
        String message = "sshd(pam_unix)[19939] failure";
        List<Argument<?>> parsedArguments = parser.parse(message);
        Parser.constructPattern(message, parsedArguments, patternedMessage, true);
        assertEquals("sshd(pam_unix)[%I] failure", patternedMessage.toString());
    }

    public void testInteriorParenExtractsNumber() throws ParseException {
        String message = "worker(4739) done";
        List<Argument<?>> parsedArguments = parser.parse(message);
        Parser.constructPattern(message, parsedArguments, patternedMessage, true);
        assertEquals("worker(%I) done", patternedMessage.toString());
    }

    public void testBracketWrappedIpv4StillRecognized() throws ParseException {
        // regression guard: a bracket at a token EDGE still strips (empty sub-token skipped) so the IPv4 matches
        String message = "from [192.168.1.1] port 22";
        List<Argument<?>> parsedArguments = parser.parse(message);
        Parser.constructPattern(message, parsedArguments, patternedMessage, true);
        assertEquals("from [%4] port %I", patternedMessage.toString());
    }

    public void testCommaStaysBoundaryNotDelimiter() throws ParseException {
        // regression guard: ',' remains a boundary char (not reclassified), so its behavior is unchanged
        String message = "close, 5 bytes";
        List<Argument<?>> parsedArguments = parser.parse(message);
        Parser.constructPattern(message, parsedArguments, patternedMessage, true);
        assertEquals("close, %I bytes", patternedMessage.toString());
    }

    public void testConsecutiveInteriorBracketsWrapNumber() throws ParseException {
        // corner case: a run of consecutive interior boundary chars before the value ("[(") - the whole run stays
        // literal and only the wrapped number is extracted
        String message = "sshd[(19939)] failure";
        List<Argument<?>> parsedArguments = parser.parse(message);
        Parser.constructPattern(message, parsedArguments, patternedMessage, true);
        assertEquals("sshd[(%I)] failure", patternedMessage.toString());
    }

    public void testConsecutiveEdgeBracketsWrapIpv4() throws ParseException {
        // regression guard: consecutive boundary chars at a token EDGE are all stripped as prefixes/suffix, so the
        // wrapped IPv4 is still recognized as a single typed value
        String message = "peer [(1.2.3.4)] up";
        List<Argument<?>> parsedArguments = parser.parse(message);
        Parser.constructPattern(message, parsedArguments, patternedMessage, true);
        assertEquals("peer [(%4)] up", patternedMessage.toString());
    }

    // KNOWN LIMITATION (documented, not a defect): a typed multi-token value (IPv4, timestamp, ...) that DIRECTLY abuts
    // an interior boundary with no delimiter in between loses its compound type. An interior boundary invalidates the
    // in-progress typed token (the same rule that correctly stops "2017[12[25" being read as a date), so the value
    // decomposes into its component sub-tokens. Crucially each component is still extracted, so the template stays
    // STABLE across values (no per-value explosion) - it is just not recognized as %4/%T. Real logs avoid this by
    // separating the value from the bracket (e.g. "ip:port", or "[ip] port" with a space), so it rarely bites. These
    // tests pin the current behavior; if a future change makes such values keep their %4/%T type, update them.

    public void testTypedValueAbuttingInteriorBracketLosesCompoundType() throws ParseException {
        // IPv4 immediately followed by an interior bracket+digit: %4 decomposes to four %I, and the wrapped number is
        // still extracted - stable template, just not %4
        String message = "1.2.3.4[5] x";
        List<Argument<?>> parsedArguments = parser.parse(message);
        Parser.constructPattern(message, parsedArguments, patternedMessage, true);
        assertEquals("%I.%I.%I.%I[%I] x", patternedMessage.toString());
    }

    public void testEdgeBracketedTypedValueAbuttingContentLosesCompoundType() throws ParseException {
        // edge-bracketed IPv4 immediately followed by content (no delimiter after ']'): the ']' turns out to be interior
        // (content resumes), so %4 decomposes to four %I
        String message = "[1.2.3.4]x done";
        List<Argument<?>> parsedArguments = parser.parse(message);
        Parser.constructPattern(message, parsedArguments, patternedMessage, true);
        assertEquals("[%I.%I.%I.%I]x done", patternedMessage.toString());
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
        String message = "[Thu Oct 05 14:48:00.123 2023] closing";
        List<Argument<?>> parsedArguments = parser.parse(message);
        Parser.constructPattern(message, parsedArguments, patternedMessage, true);
        assertEquals("[%T] closing", patternedMessage.toString());
        assertThat(parsedArguments.getFirst(), instanceOf(Timestamp.class));
        assertEquals(1696517280123L, ((Timestamp) parsedArguments.getFirst()).getTimestampMillis()); // 2023-10-05T14:48:00.123Z
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

    // ------------------------------------------------------------------------------------------------------------------
    // Double / exponent coverage. Helpers assert the reconstructed pattern (and, for real doubles, the parsed value).
    // ------------------------------------------------------------------------------------------------------------------

    private List<Argument<?>> assertPattern(String message, String expectedPattern) throws ParseException {
        patternedMessage.setLength(0);
        List<Argument<?>> parsed = parser.parse(message);
        Parser.constructPattern(message, parsed, patternedMessage, true);
        assertEquals("pattern for [" + message + "]", expectedPattern, patternedMessage.toString());
        return parsed;
    }

    private void assertSingleDouble(String message, String expectedPattern, double expectedValue) throws ParseException {
        List<Argument<?>> parsed = assertPattern(message, expectedPattern);
        assertEquals("exactly one argument for [" + message + "]", 1, parsed.size());
        assertThat(parsed.getFirst(), instanceOf(DoubleArgument.class));
        assertEquals(expectedValue, ((DoubleArgument) parsed.getFirst()).value(), 0);
    }

    public void testDecimalDoubles() throws ParseException {
        assertSingleDouble("x 3.14 y", "x %F y", 3.14);
        assertSingleDouble("x 0.5 y", "x %F y", 0.5);
        assertSingleDouble("x -5.08 y", "x %F y", -5.08);
    }

    public void testSupportedExponentDoubles() throws ParseException {
        // exponent forms char recognizes today (regression guard for the interior-sign double fix)
        assertSingleDouble("x 1e-5 y", "x %F y", 1e-5);
        assertSingleDouble("x 1.5E-3 y", "x %F y", 1.5E-3);
        assertSingleDouble("x -1.09e-2 y", "x %F y", -1.09e-2);
    }

    public void testLongDecimalDoesNotOverflow() throws ParseException {
        // integer/fractional parts beyond the int-safe fast path must fall back to parseDouble (correct value, still %F).
        // regression guard for the decimal-overflow fix; before it, these produced a %F with a WRONG value.
        assertSingleDouble("x 1234567890123.5 y", "x %F y", 1234567890123.5);
        assertSingleDouble("x 1.123456789012345 y", "x %F y", 1.123456789012345);
    }

    public void testInteriorSignIsNotDouble() throws ParseException {
        // an interior sign that is NOT an exponent sign means the token is not a double (the "0-23" range family);
        // it must be emitted as its sub-tokens, never as a single %F.
        assertPattern("x 0-23 y", "x %I-%I y");
        assertPattern("x 1-2-3 y", "x %I-%I-%I y");
        assertPattern("x 5-3 y", "x %I-%I y");
        assertPattern("x 1+2 y", "x %I+%I y");
        assertPattern("x 1e5-3 y", "x %H-%I y"); // interior '-' after digits (not right after e/E) -> not a double
    }

    public void testMalformedNumberIsNotDouble() throws ParseException {
        assertPattern("x 1.2.3 y", "x %I.%I.%I y"); // two decimal points -> parseDouble fails -> sub-tokens
        assertPattern("x 1e y", "x %H y"); // bare "1e" is a single sub-token that fits hex
    }

    public void testSingleSubTokenHexNotDouble() throws ParseException {
        // a single sub-token that fits hex is a hex, even though "1e5" also reads as a double
        assertPattern("x 1e5 y", "x %H y");
    }

    public void testMultiExponentIsNotSingleDouble() throws ParseException {
        // two exponents are not a valid double; the token must never collapse to a single %F
        List<Argument<?>> parsed = parser.parse("x 1e-6E+4 y");
        boolean singleDouble = parsed.size() == 1 && parsed.getFirst() instanceof DoubleArgument;
        assertFalse("a multi-exponent token must not be a single double", singleDouble);
    }

    // Scientific-notation doubles: plain ('e5'), signed ('e+5'/'e-5') and with a fractional mantissa ('1.5e3')
    // exponents all parse to a single %F. (These required the exponent-classification rework: '+' wired like '-'
    // for the double token, and a sub-token following a double-class delimiter with no structured generator keeps
    // its generic types instead of being cleared.)
    public void testScientificNotationDoubles() throws ParseException {
        assertSingleDouble("x 1e+5 y", "x %F y", 1e+5);
        assertSingleDouble("x 1.5e3 y", "x %F y", 1.5e3);
        assertSingleDouble("x 6.022e23 y", "x %F y", 6.022e23);
        assertSingleDouble("x -2.5e+10 y", "x %F y", -2.5e+10);
    }

    // BGL/Thunderbird timestamps. The first is "<epoch-seconds> <date>" where the epoch IS the instant and the
    // trailing YYYY.MM.DD date is collapsed into the %T span but ignored; the second is a full date-time with
    // microseconds (yyyy-MM-dd-HH.mm.ss.ffffff). Before these were added char decomposed them to ints.
    public void testEpochDateTimestampCollapses() throws ParseException {
        List<Argument<?>> parsed = assertPattern("x 1117838570 2005.06.03 y", "x %T y");
        assertEquals("exactly one argument", 1, parsed.size());
        assertThat(parsed.getFirst(), instanceOf(Timestamp.class));
        // value comes straight from the epoch (seconds * 1000), NOT from parsing the date
        assertEquals(1117838570L * 1000L, ((Timestamp) parsed.getFirst()).getTimestampMillis());
    }

    public void testBglDateTimeCollapses() throws ParseException {
        assertPattern("x 2005-06-03-15.42.50.363779 y", "x %T y");
    }

    public void testBglTimestampsInContext() throws ParseException {
        // real BGL line prefix: leading '-' is a lone sign char and stays literal (NOT an argument), then epoch+date
        // -> %T, then the full datetime -> %T
        assertPattern("- 1117838570 2005.06.03 node 2005-06-03-15.42.50.363779 node RAS", "- %T node %T node RAS");
    }

    public void testLoneSignIsNotAnArgument() throws ParseException {
        // a sign character with no digits is not a number - it must stay literal, never a spurious %I
        assertPattern("x - y", "x - y");
        assertPattern("x + y", "x + y");
        // sign + digits is still an integer
        assertPattern("x -5 y", "x %I y");
        assertPattern("x +10 y", "x %I y");
    }

    public void testEpochTimestampGuards() throws ParseException {
        // a bare date is NOT a timestamp on its own (only the epoch makes it one)
        assertPattern("x 2005.06.03 y", "x %I.%I.%I y");
        // a lone epoch-range integer without a following date stays a plain integer
        assertPattern("x 1117838570 y", "x %I y");
        // out-of-range "date" and a 2-part decimal are unaffected
        assertPattern("x 1.2.3 y", "x %I.%I.%I y");
        assertSingleDouble("x 3.14 y", "x %F y", 3.14);
    }
}

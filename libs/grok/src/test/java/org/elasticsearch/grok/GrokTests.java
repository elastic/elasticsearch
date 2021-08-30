/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.grok;

import org.elasticsearch.core.Tuple;
import org.elasticsearch.grok.GrokCaptureConfig.NativeExtracterMap;
import org.elasticsearch.test.ESTestCase;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.DoubleConsumer;
import java.util.function.Function;
import java.util.function.IntConsumer;
import java.util.function.LongConsumer;

import static org.elasticsearch.core.Tuple.tuple;
import static org.elasticsearch.grok.GrokCaptureType.BOOLEAN;
import static org.elasticsearch.grok.GrokCaptureType.DOUBLE;
import static org.elasticsearch.grok.GrokCaptureType.FLOAT;
import static org.elasticsearch.grok.GrokCaptureType.INTEGER;
import static org.elasticsearch.grok.GrokCaptureType.LONG;
import static org.elasticsearch.grok.GrokCaptureType.STRING;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;


public class GrokTests extends ESTestCase {

    public void testMatchWithoutCaptures() {
        testMatchWithoutCaptures(false);
        testMatchWithoutCaptures(true);
    }

    private void testMatchWithoutCaptures(boolean ecsCompatibility) {
        Grok grok = new Grok(Grok.getBuiltinPatterns(ecsCompatibility), "value", logger::warn);
        assertThat(grok.captures("value"), equalTo(Map.of()));
        assertThat(grok.captures("prefix_value"), equalTo(Map.of()));
        assertThat(grok.captures("no_match"), nullValue());
    }

    public void testCapturesBytes() {
        testCapturesBytes(false);
        testCapturesBytes(true);
    }

    private void testCapturesBytes(boolean ecsCompatibility) {
        Grok grok = new Grok(Grok.getBuiltinPatterns(ecsCompatibility), "%{NUMBER:n:int}", logger::warn);
        byte[] utf8 = "10".getBytes(StandardCharsets.UTF_8);
        assertThat(captureBytes(grok, utf8, 0, utf8.length), equalTo(Map.of("n", 10)));
        assertThat(captureBytes(grok, utf8, 0, 1), equalTo(Map.of("n", 1)));
        utf8 = "10 11 12".getBytes(StandardCharsets.UTF_8);
        assertThat(captureBytes(grok, utf8, 0, 2), equalTo(Map.of("n", 10)));
        assertThat(captureBytes(grok, utf8, 3, 2), equalTo(Map.of("n", 11)));
        assertThat(captureBytes(grok, utf8, 6, 2), equalTo(Map.of("n", 12)));
    }

    private Map<String, Object> captureBytes(Grok grok, byte[] utf8, int offset, int length) {
        GrokCaptureExtracter.MapExtracter extracter = new GrokCaptureExtracter.MapExtracter(grok.captureConfig());
        if (grok.match(utf8, offset, length, extracter)) {
            return extracter.result();
        }
        return null;
    }

    public void testNoMatchingPatternInDictionary() {
        Exception e = expectThrows(IllegalArgumentException.class, () -> new Grok(Collections.emptyMap(), "%{NOTFOUND}", logger::warn));
        assertThat(e.getMessage(), equalTo("Unable to find pattern [NOTFOUND] in Grok's pattern dictionary"));
    }

    public void testSimpleSyslogLine() {
        final String logSource = "evita";
        final String timestamp = "Mar 16 00:01:25";
        final String message = "connect from camomile.cloud9.net[168.100.1.3]";
        final String program = "postfix/smtpd";

        testSimpleSyslogLine(
            false,
            tuple(Map.entry("facility", STRING), null),
            tuple(Map.entry("logsource", STRING), logSource),
            tuple(Map.entry("message", STRING), message),
            tuple(Map.entry("pid", STRING), "1713"),
            tuple(Map.entry("priority", STRING), null),
            tuple(Map.entry("program", STRING), program),
            tuple(Map.entry("timestamp", STRING), timestamp),
            tuple(Map.entry("timestamp8601", STRING), null),
            List.of()
        );

        testSimpleSyslogLine(
            true,
            tuple(Map.entry("log.syslog.facility.code", INTEGER), null),
            tuple(Map.entry("host.hostname", STRING), logSource),
            tuple(Map.entry("message", STRING), message),
            tuple(Map.entry("process.pid", INTEGER), 1713),
            tuple(Map.entry("log.syslog.priority", INTEGER), null),
            tuple(Map.entry("process.name", STRING), program),
            tuple(Map.entry("timestamp", STRING), timestamp),
            null,
            List.of("timestamp")
        );
    }

    private void testSimpleSyslogLine(
        boolean ecsCompatibility,
        Tuple<Map.Entry<String, GrokCaptureType>, Object> facility,
        Tuple<Map.Entry<String, GrokCaptureType>, Object> logSource,
        Tuple<Map.Entry<String, GrokCaptureType>, Object> message,
        Tuple<Map.Entry<String, GrokCaptureType>, Object> pid,
        Tuple<Map.Entry<String, GrokCaptureType>, Object> priority,
        Tuple<Map.Entry<String, GrokCaptureType>, Object> program,
        Tuple<Map.Entry<String, GrokCaptureType>, Object> timestamp,
        Tuple<Map.Entry<String, GrokCaptureType>, Object> timestamp8601,
        List<String> acceptedDuplicates
    ) {
        String line = "Mar 16 00:01:25 evita postfix/smtpd[1713]: connect from camomile.cloud9.net[168.100.1.3]";
        Grok grok = new Grok(Grok.getBuiltinPatterns(ecsCompatibility), "%{SYSLOGLINE}", logger::warn);

        Map<String, GrokCaptureType> captureTypes = new HashMap<>();
        captureTypes.put(facility.v1().getKey(), facility.v1().getValue());
        captureTypes.put(logSource.v1().getKey(), logSource.v1().getValue());
        captureTypes.put(message.v1().getKey(), message.v1().getValue());
        captureTypes.put(pid.v1().getKey(), pid.v1().getValue());
        captureTypes.put(priority.v1().getKey(), priority.v1().getValue());
        captureTypes.put(program.v1().getKey(), program.v1().getValue());
        captureTypes.put(timestamp.v1().getKey(), timestamp.v1().getValue());
        if (timestamp8601 != null) {
            captureTypes.put(timestamp8601.v1().getKey(), timestamp8601.v1().getValue());
        }

        assertCaptureConfig(grok, captureTypes, acceptedDuplicates);
        Map<String, Object> matches = grok.captures(line);
        assertEquals(logSource.v2(), matches.get(logSource.v1().getKey()));
        assertEquals(timestamp.v2(), matches.get(timestamp.v1().getKey()));
        assertEquals(message.v2(), matches.get(message.v1().getKey()));
        assertEquals(program.v2(), matches.get(program.v1().getKey()));
        assertEquals(pid.v2(), matches.get(pid.v1().getKey()));

        String[] logsource = new String[1];
        GrokCaptureExtracter logsourceExtracter =
            namedConfig(grok, logSource.v1().getKey())
                .nativeExtracter(new ThrowingNativeExtracterMap() {
                    @Override
                    public GrokCaptureExtracter forString(Function<Consumer<String>, GrokCaptureExtracter> buildExtracter) {
                        return buildExtracter.apply(str -> logsource[0] = str);
                    }
                });
        assertThat(specificCapture(grok, line, logsourceExtracter), is(true));
        assertThat(logsource[0], equalTo(logSource.v2()));
    }

    public void testSyslog5424Line() {
        final String ts = "2009-06-30T18:30:00+02:00";
        final String host = "paxton.local";
        final String app = "grokdebug";
        final String sd = "[id1 foo=\\\"bar\\\"][id2 baz=\\\"something\\\"]";
        final String msg = "Hello, syslog.";
        final String ver = "1";

        testSyslog5424Line(
            false,
            tuple(Map.entry("syslog5424_app", STRING), app),
            tuple(Map.entry("syslog5424_host", STRING), host),
            tuple(Map.entry("syslog5424_msg", STRING), msg),
            tuple(Map.entry("syslog5424_msgid", STRING), null),
            tuple(Map.entry("syslog5424_pri", STRING), "191"),
            tuple(Map.entry("syslog5424_proc", STRING), "4123"),
            tuple(Map.entry("syslog5424_sd", STRING), sd),
            tuple(Map.entry("syslog5424_ts", STRING), ts),
            tuple(Map.entry("syslog5424_ver", STRING), ver)
        );
        testSyslog5424Line(
            true,
            tuple(Map.entry("process.name", STRING), app),
            tuple(Map.entry("host.hostname", STRING), host),
            tuple(Map.entry("message", STRING), msg),
            tuple(Map.entry("event.code", STRING), null),
            tuple(Map.entry("log.syslog.priority", INTEGER), 191),
            tuple(Map.entry("process.pid", INTEGER), 4123),
            tuple(Map.entry("system.syslog.structured_data", STRING), sd),
            tuple(Map.entry("timestamp", STRING), ts),
            tuple(Map.entry("system.syslog.version", STRING), ver)
        );
    }

    private void testSyslog5424Line(
        boolean ecsCompatibility,
        Tuple<Map.Entry<String, GrokCaptureType>, Object> app,
        Tuple<Map.Entry<String, GrokCaptureType>, Object> host,
        Tuple<Map.Entry<String, GrokCaptureType>, Object> msg,
        Tuple<Map.Entry<String, GrokCaptureType>, Object> msgid,
        Tuple<Map.Entry<String, GrokCaptureType>, Object> pri,
        Tuple<Map.Entry<String, GrokCaptureType>, Object> proc,
        Tuple<Map.Entry<String, GrokCaptureType>, Object> sd,
        Tuple<Map.Entry<String, GrokCaptureType>, Object> ts,
        Tuple<Map.Entry<String, GrokCaptureType>, Object> ver
    ) {
        String line = "<191>1 2009-06-30T18:30:00+02:00 paxton.local grokdebug 4123 - [id1 foo=\\\"bar\\\"][id2 baz=\\\"something\\\"] " +
            "Hello, syslog.";
        Grok grok = new Grok(Grok.getBuiltinPatterns(ecsCompatibility), "%{SYSLOG5424LINE}", logger::warn);
        assertCaptureConfig(
            grok,
            Map.ofEntries(app.v1(), host.v1(), msg.v1(), msgid.v1(), pri.v1(), proc.v1(), sd.v1(), ts.v1(), ver.v1())
        );
        Map<String, Object> matches = grok.captures(line);
        assertEquals(pri.v2(), matches.get(pri.v1().getKey()));
        assertEquals(ver.v2(), matches.get(ver.v1().getKey()));
        assertEquals(ts.v2(), matches.get(ts.v1().getKey()));
        assertEquals(host.v2(), matches.get(host.v1().getKey()));
        assertEquals(app.v2(), matches.get(app.v1().getKey()));
        assertEquals(proc.v2(), matches.get(proc.v1().getKey()));
        assertEquals(msgid.v2(), matches.get(msgid.v1().getKey()));
        assertEquals(sd.v2(), matches.get(sd.v1().getKey()));
        assertEquals(msg.v2(), matches.get(msg.v1().getKey()));
    }

    public void testDatePattern() {
        testDatePattern(false);
        testDatePattern(true);
    }

    private void testDatePattern(boolean ecsCompatibility) {
        String line = "fancy 12-12-12 12:12:12";
        Grok grok = new Grok(Grok.getBuiltinPatterns(ecsCompatibility), "(?<timestamp>%{DATE_EU} %{TIME})", logger::warn);
        assertCaptureConfig(grok, Map.of("timestamp", STRING));
        Map<String, Object> matches = grok.captures(line);
        assertEquals("12-12-12 12:12:12", matches.get("timestamp"));
    }

    public void testNilCoercedValues() {
        testNilCoercedValues(false);
        testNilCoercedValues(true);
    }

    private void testNilCoercedValues(boolean ecsCompatibility) {
        Grok grok = new Grok(Grok.getBuiltinPatterns(ecsCompatibility), "test (N/A|%{BASE10NUM:duration:float}ms)", logger::warn);
        assertCaptureConfig(grok, Map.of("duration", FLOAT));
        Map<String, Object> matches = grok.captures("test 28.4ms");
        assertEquals(28.4f, matches.get("duration"));
        matches = grok.captures("test N/A");
        assertEquals(null, matches.get("duration"));
    }

    public void testNilWithNoCoercion() {
        testNilWithNoCoercion(false);
        testNilWithNoCoercion(true);
    }

    private void testNilWithNoCoercion(boolean ecsCompatibility) {
        Grok grok = new Grok(Grok.getBuiltinPatterns(ecsCompatibility), "test (N/A|%{BASE10NUM:duration}ms)", logger::warn);
        assertCaptureConfig(grok, Map.of("duration", STRING));
        Map<String, Object> matches = grok.captures("test 28.4ms");
        assertEquals("28.4", matches.get("duration"));
        matches = grok.captures("test N/A");
        assertEquals(null, matches.get("duration"));
    }

    public void testUnicodeSyslog() {
        testUnicodeSyslog(false);
        testUnicodeSyslog(true);
    }

    private void testUnicodeSyslog(boolean ecsCompatibility) {
        Grok grok = new Grok(
            Grok.getBuiltinPatterns(ecsCompatibility),
            "<%{POSINT:syslog_pri}>%{SPACE}%{SYSLOGTIMESTAMP:syslog_timestamp} " +
                "%{SYSLOGHOST:syslog_hostname} %{PROG:syslog_program}(:?)(?:\\[%{GREEDYDATA:syslog_pid}\\])?(:?) " +
                "%{GREEDYDATA:syslog_message}", logger::warn
        );
        assertCaptureConfig(
            grok,
            Map.ofEntries(
                Map.entry("syslog_hostname", STRING),
                Map.entry("syslog_message", STRING),
                Map.entry("syslog_pid", STRING),
                Map.entry("syslog_pri", STRING),
                Map.entry("syslog_program", STRING),
                Map.entry("syslog_timestamp", STRING)
            )
        );
        Map<String, Object> matches = grok.captures("<22>Jan  4 07:50:46 mailmaster postfix/policy-spf[9454]: : " +
                "SPF permerror (Junk encountered in record 'v=spf1 mx a:mail.domain.no ip4:192.168.0.4 �all'): Envelope-from: " +
                "email@domain.no");
        assertThat(matches.get("syslog_pri"), equalTo("22"));
        assertThat(matches.get("syslog_program"), equalTo("postfix/policy-spf"));
        assertThat(matches.get("tags"), nullValue());
    }

    public void testNamedFieldsWithWholeTextMatch() {
        testNamedFieldsWithWholeTextMatch(false);
        testNamedFieldsWithWholeTextMatch(true);
    }

    private void testNamedFieldsWithWholeTextMatch(boolean ecsCompatibility) {
        Grok grok = new Grok(Grok.getBuiltinPatterns(ecsCompatibility), "%{DATE_EU:stimestamp}", logger::warn);
        assertCaptureConfig(grok, Map.of("stimestamp", STRING));
        Map<String, Object> matches = grok.captures("11/01/01");
        assertThat(matches.get("stimestamp"), equalTo("11/01/01"));
    }

    public void testWithOniguramaNamedCaptures() {
        testWithOniguramaNamedCaptures(false);
        testWithOniguramaNamedCaptures(true);
    }

    private void testWithOniguramaNamedCaptures(boolean ecsCompatibility) {
        Grok grok = new Grok(Grok.getBuiltinPatterns(ecsCompatibility), "(?<foo>\\w+)", logger::warn);
        assertCaptureConfig(grok, Map.of("foo", STRING));
        Map<String, Object> matches = grok.captures("hello world");
        assertThat(matches.get("foo"), equalTo("hello"));
    }

    public void testISO8601() {
        testISO8601(false);
        testISO8601(true);
    }

    private void testISO8601(boolean ecsCompatibility) {
        Grok grok = new Grok(Grok.getBuiltinPatterns(ecsCompatibility), "^%{TIMESTAMP_ISO8601}$", logger::warn);
        assertCaptureConfig(grok, Map.of());
        List<String> timeMessages = Arrays.asList(
                "2001-01-01T00:00:00",
                "1974-03-02T04:09:09",
                "2010-05-03T08:18:18+00:00",
                "2004-07-04T12:27:27-00:00",
                "2001-09-05T16:36:36+0000",
                "2001-11-06T20:45:45-0000",
                "2001-12-07T23:54:54Z",
                "2001-01-01T00:00:00.123456",
                "1974-03-02T04:09:09.123456",
                "2010-05-03T08:18:18.123456+00:00",
                "2004-07-04T12:27:27.123456-00:00",
                "2001-09-05T16:36:36.123456+0000",
                "2001-11-06T20:45:45.123456-0000",
                "2001-12-07T23:54:54.123456Z",
                "2001-12-07T23:54:60.123456Z" // '60' second is a leap second.
        );
        for (String msg : timeMessages) {
            assertThat(grok.match(msg), is(true));
        }
    }

    public void testNotISO8601() {
        testNotISO8601(false, List.of("2001-01-01T0:00:00")); // legacy patterns do not permit single-digit hours
        testNotISO8601(true, List.of());
    }

    private void testNotISO8601(boolean ecsCompatibility, List<String> additionalCases) {
        Grok grok = new Grok(Grok.getBuiltinPatterns(ecsCompatibility), "^%{TIMESTAMP_ISO8601}$", logger::warn);
        assertCaptureConfig(grok, Map.of());
        List<String> timeMessages = Arrays.asList(
                "2001-13-01T00:00:00", // invalid month
                "2001-00-01T00:00:00", // invalid month
                "2001-01-00T00:00:00", // invalid day
                "2001-01-32T00:00:00", // invalid day
                "2001-01-aT00:00:00", // invalid day
                "2001-01-1aT00:00:00", // invalid day
                "2001-01-01Ta0:00:00", // invalid hour
                "2001-01-01T25:00:00", // invalid hour
                "2001-01-01T01:60:00", // invalid minute
                "2001-01-01T00:aa:00", // invalid minute
                "2001-01-01T00:00:aa", // invalid second
                "2001-01-01T00:00:-1", // invalid second
                "2001-01-01T00:00:61", // invalid second
                "2001-01-01T00:00:00A", // invalid timezone
                "2001-01-01T00:00:00+", // invalid timezone
                "2001-01-01T00:00:00+25", // invalid timezone
                "2001-01-01T00:00:00+2500", // invalid timezone
                "2001-01-01T00:00:00+25:00", // invalid timezone
                "2001-01-01T00:00:00-25", // invalid timezone
                "2001-01-01T00:00:00-2500", // invalid timezone
                "2001-01-01T00:00:00-00:61" // invalid timezone
        );
        List<String> timesToTest = new ArrayList<>(timeMessages);
        timesToTest.addAll(additionalCases);
        for (String msg : timesToTest) {
            assertThat(grok.match(msg), is(false));
        }
    }

    public void testNoNamedCaptures() {
        Map<String, String> bank = new HashMap<>();

        bank.put("NAME", "Tal");
        bank.put("EXCITED_NAME", "!!!%{NAME:name}!!!");
        bank.put("TEST", "hello world");

        String text = "wowza !!!Tal!!! - Tal";
        String pattern = "%{EXCITED_NAME} - %{NAME}";
        Grok g = new Grok(bank, pattern, false, logger::warn);
        assertCaptureConfig(g, Map.of("EXCITED_NAME_0", STRING, "NAME_21", STRING, "NAME_22", STRING));

        assertEquals("(?<EXCITED_NAME_0>!!!(?<NAME_21>Tal)!!!) - (?<NAME_22>Tal)", g.toRegex(pattern));
        assertEquals(true, g.match(text));

        Object actual = g.captures(text);
        Map<String, Object> expected = new HashMap<>();
        expected.put("EXCITED_NAME_0", "!!!Tal!!!");
        expected.put("NAME_21", "Tal");
        expected.put("NAME_22", "Tal");
        assertEquals(expected, actual);
    }

    public void testCircularReference() {
        Exception e = expectThrows(IllegalArgumentException.class, () -> {
            Map<String, String> bank = new HashMap<>();
            bank.put("NAME", "!!!%{NAME}!!!");
            String pattern = "%{NAME}";
            new Grok(bank, pattern, false, logger::warn);
        });
        assertEquals("circular reference in pattern [NAME][!!!%{NAME}!!!]", e.getMessage());

        e = expectThrows(IllegalArgumentException.class, () -> {
            Map<String, String> bank = new HashMap<>();
            bank.put("NAME", "!!!%{NAME:name}!!!");
            String pattern = "%{NAME}";
            new Grok(bank, pattern, false, logger::warn);
        });
        assertEquals("circular reference in pattern [NAME][!!!%{NAME:name}!!!]", e.getMessage());

        e = expectThrows(IllegalArgumentException.class, () -> {
            Map<String, String> bank = new HashMap<>();
            bank.put("NAME", "!!!%{NAME:name:int}!!!");
            String pattern = "%{NAME}";
            new Grok(bank, pattern, false, logger::warn);
        });
        assertEquals("circular reference in pattern [NAME][!!!%{NAME:name:int}!!!]", e.getMessage());

        e = expectThrows(IllegalArgumentException.class, () -> {
            Map<String, String> bank = new TreeMap<>();
            bank.put("NAME1", "!!!%{NAME2}!!!");
            bank.put("NAME2", "!!!%{NAME1}!!!");
            String pattern = "%{NAME1}";
            new Grok(bank, pattern, false, logger::warn);
        });
        assertEquals("circular reference in pattern [NAME2][!!!%{NAME1}!!!] back to pattern [NAME1]", e.getMessage());

        e = expectThrows(IllegalArgumentException.class, () -> {
            Map<String, String> bank = new TreeMap<>();
            bank.put("NAME1", "!!!%{NAME2}!!!");
            bank.put("NAME2", "!!!%{NAME3}!!!");
            bank.put("NAME3", "!!!%{NAME1}!!!");
            String pattern = "%{NAME1}";
            new Grok(bank, pattern, false, logger::warn);
        });
        assertEquals("circular reference in pattern [NAME3][!!!%{NAME1}!!!] back to pattern [NAME1] via patterns [NAME2]", e.getMessage());

        e = expectThrows(IllegalArgumentException.class, () -> {
            Map<String, String> bank = new TreeMap<>();
            bank.put("NAME1", "!!!%{NAME2}!!!");
            bank.put("NAME2", "!!!%{NAME3}!!!");
            bank.put("NAME3", "!!!%{NAME4}!!!");
            bank.put("NAME4", "!!!%{NAME5}!!!");
            bank.put("NAME5", "!!!%{NAME1}!!!");
            String pattern = "%{NAME1}";
            new Grok(bank, pattern, false, logger::warn);
        });
        assertEquals(
            "circular reference in pattern [NAME5][!!!%{NAME1}!!!] back to pattern [NAME1] via patterns [NAME2=>NAME3=>NAME4]",
            e.getMessage()
        );
    }

    public void testCircularSelfReference() {
        Exception e = expectThrows(IllegalArgumentException.class, () -> {
            Map<String, String> bank = new HashMap<>();
            bank.put("ANOTHER", "%{INT}");
            bank.put("INT", "%{INT}");
            String pattern = "does_not_matter";
            new Grok(bank, pattern, false, logger::warn);
        });
        assertEquals("circular reference in pattern [INT][%{INT}]", e.getMessage());
    }

    public void testBooleanCaptures() {
        testBooleanCaptures(false);
        testBooleanCaptures(true);
    }

    private void testBooleanCaptures(boolean ecsCompatibility) {
        String pattern = "%{WORD:name}=%{WORD:status:boolean}";
        Grok g = new Grok(Grok.getBuiltinPatterns(ecsCompatibility), pattern, logger::warn);
        assertCaptureConfig(g, Map.of("name", STRING, "status", BOOLEAN));

        String text = "active=true";
        Map<String, Object> expected = new HashMap<>();
        expected.put("name", "active");
        expected.put("status", true);
        Map<String, Object> actual = g.captures(text);

        assertEquals(expected, actual);

        boolean[] status = new boolean[1];
        GrokCaptureExtracter statusExtracter = namedConfig(g, "status").nativeExtracter(new ThrowingNativeExtracterMap() {
            @Override
            public GrokCaptureExtracter forBoolean(Function<Consumer<Boolean>, GrokCaptureExtracter> buildExtracter) {
                return buildExtracter.apply(b -> status[0] = b);
            }
        });
        assertThat(specificCapture(g, text, statusExtracter), is(true));
        assertThat(status[0], equalTo(true));
    }

    public void testNumericCaptures() {
        Map<String, String> bank = new HashMap<>();
        bank.put("BASE10NUM", "(?<![0-9.+-])(?>[+-]?(?:(?:[0-9]+(?:\\.[0-9]+)?)|(?:\\.[0-9]+)))");
        bank.put("NUMBER", "(?:%{BASE10NUM})");

        String pattern = "%{NUMBER:bytes:float} %{NUMBER:id:long} %{NUMBER:rating:double}";
        Grok g = new Grok(bank, pattern, logger::warn);
        assertCaptureConfig(g, Map.of("bytes", FLOAT, "id", LONG, "rating", DOUBLE));

        String text = "12009.34 20000000000 4820.092";
        Map<String, Object> expected = new HashMap<>();
        expected.put("bytes", 12009.34f);
        expected.put("id", 20000000000L);
        expected.put("rating", 4820.092);
        Map<String, Object> actual = g.captures(text);

        assertEquals(expected, actual);

        float[] bytes = new float[1];
        GrokCaptureExtracter bytesExtracter = namedConfig(g, "bytes").nativeExtracter(new ThrowingNativeExtracterMap() {
            @Override
            public GrokCaptureExtracter forFloat(Function<FloatConsumer, GrokCaptureExtracter> buildExtracter) {
                return buildExtracter.apply(f -> bytes[0] = f);
            }
        });
        assertThat(specificCapture(g, text, bytesExtracter), is(true));
        assertThat(bytes[0], equalTo(12009.34f));

        long[] id = new long[1];
        GrokCaptureExtracter idExtracter = namedConfig(g, "id").nativeExtracter(new ThrowingNativeExtracterMap() {
            @Override
            public GrokCaptureExtracter forLong(Function<LongConsumer, GrokCaptureExtracter> buildExtracter) {
                return buildExtracter.apply(l -> id[0] = l);
            }
        });
        assertThat(specificCapture(g, text, idExtracter), is(true));
        assertThat(id[0], equalTo(20000000000L));

        double[] rating = new double[1];
        GrokCaptureExtracter ratingExtracter = namedConfig(g, "rating").nativeExtracter(new ThrowingNativeExtracterMap() {
            public GrokCaptureExtracter forDouble(java.util.function.Function<DoubleConsumer,GrokCaptureExtracter> buildExtracter) {
                return buildExtracter.apply(d -> rating[0] = d);
            }
        });
        assertThat(specificCapture(g, text, ratingExtracter), is(true));
        assertThat(rating[0], equalTo(4820.092));
    }

    public void testNumericCapturesCoercion() {
        Map<String, String> bank = new HashMap<>();
        bank.put("BASE10NUM", "(?<![0-9.+-])(?>[+-]?(?:(?:[0-9]+(?:\\.[0-9]+)?)|(?:\\.[0-9]+)))");
        bank.put("NUMBER", "(?:%{BASE10NUM})");

        String pattern = "%{NUMBER:bytes:float} %{NUMBER:status} %{NUMBER}";
        Grok g = new Grok(bank, pattern, logger::warn);
        assertCaptureConfig(g, Map.of("bytes", FLOAT, "status", STRING));

        String text = "12009.34 200 9032";
        Map<String, Object> expected = new HashMap<>();
        expected.put("bytes", 12009.34f);
        expected.put("status", "200");
        Map<String, Object> actual = g.captures(text);

        assertEquals(expected, actual);
    }

    public void testGarbageTypeNameBecomesString() {
        Map<String, String> bank = new HashMap<>();
        bank.put("BASE10NUM", "(?<![0-9.+-])(?>[+-]?(?:(?:[0-9]+(?:\\.[0-9]+)?)|(?:\\.[0-9]+)))");
        bank.put("NUMBER", "(?:%{BASE10NUM})");

        String pattern = "%{NUMBER:f:not_a_valid_type}";
        Grok g = new Grok(bank, pattern, logger::warn);
        assertCaptureConfig(g, Map.of("f", STRING));
        assertThat(g.captures("12009.34"), equalTo(Map.of("f", "12009.34")));
    }

    public void testApacheLog() {
        final String agent = "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/30.0.1599.12785 " +
            "YaBrowser/13.12.1599.12785 Safari/537.36";
        final String clientIp = "31.184.238.164";
        final String timestamp = "24/Jul/2014:05:35:37 +0530";
        final String verb = "GET";
        final String request = "/logs/access.log";
        final String httpVersion = "1.0";
        final String referrer = "http://8rursodiol.enjin.com";

        testApacheLog(
            false,
            tuple(Map.entry("agent", STRING), "\"" + agent + "\""),
            tuple(Map.entry("auth", STRING), "-"),
            tuple(Map.entry("bytes", STRING), "69849"),
            tuple(Map.entry("clientip", STRING), clientIp),
            tuple(Map.entry("httpversion", STRING), httpVersion),
            tuple(Map.entry("ident", STRING), "-"),
            tuple(Map.entry("rawrequest", STRING), null),
            tuple(Map.entry("referrer", STRING), "\"" + referrer + "\""),
            tuple(Map.entry("request", STRING), request),
            tuple(Map.entry("timestamp", STRING), timestamp),
            tuple(Map.entry("verb", STRING), verb),
            List.of(tuple(Map.entry("response", STRING), "200"))
        );
        testApacheLog(
            true,
            tuple(Map.entry("user_agent.original", STRING), agent),
            tuple(Map.entry("user.name", STRING), null),
            tuple(Map.entry("http.response.body.bytes", LONG), 69849),
            tuple(Map.entry("source.address", STRING), clientIp),
            tuple(Map.entry("http.version", STRING), httpVersion),
            tuple(Map.entry("apache.access.user.identity", STRING), null),
            tuple(Map.entry("http.response.status_code", INTEGER), 200),
            tuple(Map.entry("http.request.referrer", STRING), referrer),
            tuple(Map.entry("url.original", STRING), request),
            tuple(Map.entry("timestamp", STRING), timestamp),
            tuple(Map.entry("http.request.method", STRING), verb),
            List.of()
        );
    }

    public void testApacheLog(
        boolean ecsCompatibility,
        Tuple<Map.Entry<String, GrokCaptureType>, Object> agent,
        Tuple<Map.Entry<String, GrokCaptureType>, Object> auth,
        Tuple<Map.Entry<String, GrokCaptureType>, Object> bytes,
        Tuple<Map.Entry<String, GrokCaptureType>, Object> clientIp,
        Tuple<Map.Entry<String, GrokCaptureType>, Object> httpVersion,
        Tuple<Map.Entry<String, GrokCaptureType>, Object> ident,
        Tuple<Map.Entry<String, GrokCaptureType>, Object> rawRequest,
        Tuple<Map.Entry<String, GrokCaptureType>, Object> referrer,
        Tuple<Map.Entry<String, GrokCaptureType>, Object> request,
        Tuple<Map.Entry<String, GrokCaptureType>, Object> timestamp,
        Tuple<Map.Entry<String, GrokCaptureType>, Object> verb,
        List<Tuple<Map.Entry<String, GrokCaptureType>, Object>> additionalFields
    ) {
        String logLine = "31.184.238.164 - - [24/Jul/2014:05:35:37 +0530] \"GET /logs/access.log HTTP/1.0\" 200 69849 " +
                "\"http://8rursodiol.enjin.com\" \"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) " +
                "Chrome/30.0.1599.12785 YaBrowser/13.12.1599.12785 Safari/537.36\" \"www.dlwindianrailways.com\"";
        Grok grok = new Grok(Grok.getBuiltinPatterns(ecsCompatibility), "%{COMBINEDAPACHELOG}", logger::warn);

        Map<String, GrokCaptureType> captureTypes = new HashMap<>();
        captureTypes.put(agent.v1().getKey(), agent.v1().getValue());
        captureTypes.put(auth.v1().getKey(), auth.v1().getValue());
        captureTypes.put(bytes.v1().getKey(), bytes.v1().getValue());
        captureTypes.put(clientIp.v1().getKey(), clientIp.v1().getValue());
        captureTypes.put(httpVersion.v1().getKey(), httpVersion.v1().getValue());
        captureTypes.put(ident.v1().getKey(), ident.v1().getValue());
        captureTypes.put(rawRequest.v1().getKey(), rawRequest.v1().getValue());
        captureTypes.put(referrer.v1().getKey(), referrer.v1().getValue());
        captureTypes.put(request.v1().getKey(), request.v1().getValue());
        captureTypes.put(timestamp.v1().getKey(), timestamp.v1().getValue());
        captureTypes.put(verb.v1().getKey(), verb.v1().getValue());
        for (var additionalField : additionalFields) {
            captureTypes.put(additionalField.v1().getKey(), additionalField.v1().getValue());
        }

        assertCaptureConfig(grok, captureTypes);
        Map<String, Object> matches = grok.captures(logLine);

        assertEquals(clientIp.v2(), matches.get(clientIp.v1().getKey()));
        assertEquals(ident.v2(), matches.get(ident.v1().getKey()));
        assertEquals(auth.v2(), matches.get(auth.v1().getKey()));
        assertEquals(timestamp.v2(), matches.get(timestamp.v1().getKey()));
        assertEquals(verb.v2(), matches.get(verb.v1().getKey()));
        assertEquals(request.v2(), matches.get(request.v1().getKey()));
        assertEquals(httpVersion.v2(), matches.get(httpVersion.v1().getKey()));
        assertEquals(bytes.v2(), matches.get(bytes.v1().getKey()));
        assertEquals(referrer.v2(), matches.get(referrer.v1().getKey()));
        assertEquals(null, matches.get("port"));
        assertEquals(agent.v2(), matches.get(agent.v1().getKey()));
        assertEquals(rawRequest.v2(), matches.get(rawRequest.v1().getKey()));
        for (var additionalField : additionalFields) {
            assertEquals(additionalField.v2(), matches.get(additionalField.v1().getKey()));
        }
    }

    public void testComplete() {
        Map<String, String> bank = new HashMap<>();
        bank.put("MONTHDAY", "(?:(?:0[1-9])|(?:[12][0-9])|(?:3[01])|[1-9])");
        bank.put("MONTH", "\\b(?:Jan(?:uary|uar)?|Feb(?:ruary|ruar)?|M(?:a|ä)?r(?:ch|z)?|Apr(?:il)?|Ma(?:y|i)?|Jun(?:e|i)" +
                "?|Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|O(?:c|k)?t(?:ober)?|Nov(?:ember)?|De(?:c|z)(?:ember)?)\\b");
        bank.put("MINUTE", "(?:[0-5][0-9])");
        bank.put("YEAR", "(?>\\d\\d){1,2}");
        bank.put("HOUR", "(?:2[0123]|[01]?[0-9])");
        bank.put("SECOND", "(?:(?:[0-5]?[0-9]|60)(?:[:.,][0-9]+)?)");
        bank.put("TIME", "(?!<[0-9])%{HOUR}:%{MINUTE}(?::%{SECOND})(?![0-9])");
        bank.put("INT", "(?:[+-]?(?:[0-9]+))");
        bank.put("HTTPDATE", "%{MONTHDAY}/%{MONTH}/%{YEAR}:%{TIME} %{INT}");
        bank.put("WORD", "\\b\\w+\\b");
        bank.put("BASE10NUM", "(?<![0-9.+-])(?>[+-]?(?:(?:[0-9]+(?:\\.[0-9]+)?)|(?:\\.[0-9]+)))");
        bank.put("NUMBER", "(?:%{BASE10NUM})");
        bank.put("IPV6", "((([0-9A-Fa-f]{1,4}:){7}([0-9A-Fa-f]{1,4}|:))|(([0-9A-Fa-f]{1,4}:){6}(:[0-9A-Fa-f]{1,4}|((25[0-5]|2[0-4]" +
                "\\d|1\\d\\d|[1-9]?\\d)(\\.(25[0-5]|2[0-4]\\d|1\\d\\d|[1-9]?\\d)){3})|:))|(([0-9A-Fa-f]{1,4}:){5}(((:[0-9A-Fa-f]{1,4})" +
                "{1,2})|:((25[0-5]|2[0-4]\\d|1\\d\\d|[1-9]?\\d)(\\.(25[0-5]|2[0-4]\\d|1\\d\\d|[1-9]?\\d)){3})|:))|(([0-9A-Fa-f]{1,4}:)" +
                "{4}(((:[0-9A-Fa-f]{1,4}){1,3})|((:[0-9A-Fa-f]{1,4})?:((25[0-5]|2[0-4]\\d|1\\d\\d|[1-9]?\\d)(\\.(25[0-5]|2[0-4]\\d|1\\" +
                "d\\d|[1-9]?\\d)){3}))|:))|(([0-9A-Fa-f]{1,4}:){3}(((:[0-9A-Fa-f]{1,4}){1,4})|((:[0-9A-Fa-f]{1,4}){0,2}:((25[0-5]|2[0-4]" +
                "\\d|1\\d\\d|[1-9]?\\d)(\\.(25[0-5]|2[0-4]\\d|1\\d\\d|[1-9]?\\d)){3}))|:))|(([0-9A-Fa-f]{1,4}:){2}(((:[0-9A-Fa-f]{1,4})" +
                "{1,5})" +
                "|((:[0-9A-Fa-f]{1,4}){0,3}:((25[0-5]|2[0-4]\\d|1\\d\\d|[1-9]?\\d)(\\.(25[0-5]|2[0-4]\\d|1\\d\\d|[1-9]?\\d)){3}))|:))" +
                "|(([0-9A-Fa-f]{1,4}:){1}(((:[0-9A-Fa-f]{1,4}){1,6})|((:[0-9A-Fa-f]{1,4}){0,4}:((25[0-5]|2[0-4]\\d|1\\d\\d|[1-9]?\\d)" +
                "(\\.(25[0-5]|2[0-4]\\d|1\\d\\d|[1-9]?\\d)){3}))|:))|(:(((:[0-9A-Fa-f]{1,4}){1,7})|((:[0-9A-Fa-f]{1,4}){0,5}" +
                ":((25[0-5]|2[0-4]\\d|1\\d\\d|[1-9]?\\d)(\\.(25[0-5]|2[0-4]\\d|1\\d\\d|[1-9]?\\d)){3}))|:)))(%.+)?");
        bank.put("IPV4", "(?<![0-9])(?:(?:[0-1]?[0-9]{1,2}|2[0-4][0-9]|25[0-5])[.](?:[0-1]?[0-9]{1,2}|2[0-4][0-9]|25[0-5])[.]" +
                "(?:[0-1]?[0-9]{1,2}|2[0-4][0-9]|25[0-5])[.](?:[0-1]?[0-9]{1,2}|2[0-4][0-9]|25[0-5]))(?![0-9])");
        bank.put("IP", "(?:%{IPV6}|%{IPV4})");
        bank.put("HOSTNAME", "\\b(?:[0-9A-Za-z][0-9A-Za-z-]{0,62})(?:\\.(?:[0-9A-Za-z][0-9A-Za-z-]{0,62}))*(\\.?|\\b)");
        bank.put("IPORHOST", "(?:%{IP}|%{HOSTNAME})");
        bank.put("USER", "[a-zA-Z0-9._-]+");
        bank.put("DATA", ".*?");
        bank.put("QS", "(?>(?<!\\\\)(?>\"(?>\\\\.|[^\\\\\"]+)+\"|\"\"|(?>'(?>\\\\.|[^\\\\']+)+')|''|(?>`(?>\\\\.|[^\\\\`]+)+`)|``))");

        String text = "83.149.9.216 - - [19/Jul/2015:08:13:42 +0000] \"GET /presentations/logstash-monitorama-2013/images/" +
                "kibana-dashboard3.png HTTP/1.1\" 200 171717 \"http://semicomplete.com/presentations/logstash-monitorama-2013/\" " +
                "\"Mozilla" +
                "/5.0 (Macintosh; Intel Mac OS X 10_9_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/32.0.1700.77 Safari/537.36\"";
        String pattern = "%{IPORHOST:clientip} %{USER:ident} %{USER:auth} \\[%{HTTPDATE:timestamp}\\] \"%{WORD:verb} %{DATA:request} " +
                "HTTP/%{NUMBER:httpversion}\" %{NUMBER:response:int} (?:-|%{NUMBER:bytes:int}) %{QS:referrer} %{QS:agent}";

        Grok grok = new Grok(bank, pattern, logger::warn);
        assertCaptureConfig(
            grok,
            Map.ofEntries(
                Map.entry("agent", STRING),
                Map.entry("auth", STRING),
                Map.entry("bytes", INTEGER),
                Map.entry("clientip", STRING),
                Map.entry("httpversion", STRING),
                Map.entry("ident", STRING),
                Map.entry("referrer", STRING),
                Map.entry("request", STRING),
                Map.entry("response", INTEGER),
                Map.entry("timestamp", STRING),
                Map.entry("verb", STRING)
            )
        );

        Map<String, Object> expected = new HashMap<>();
        expected.put("clientip", "83.149.9.216");
        expected.put("ident", "-");
        expected.put("auth", "-");
        expected.put("timestamp", "19/Jul/2015:08:13:42 +0000");
        expected.put("verb", "GET");
        expected.put("request", "/presentations/logstash-monitorama-2013/images/kibana-dashboard3.png");
        expected.put("httpversion", "1.1");
        expected.put("response", 200);
        expected.put("bytes", 171717);
        expected.put("referrer", "\"http://semicomplete.com/presentations/logstash-monitorama-2013/\"");
        expected.put("agent", "\"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_1) AppleWebKit/537.36 (KHTML, like Gecko) " +
                "Chrome/32.0.1700.77 Safari/537.36\"");

        Map<String, Object> actual = grok.captures(text);

        assertEquals(expected, actual);
    }

    public void testNoMatch() {
        Map<String, String> bank = new HashMap<>();
        bank.put("MONTHDAY", "(?:(?:0[1-9])|(?:[12][0-9])|(?:3[01])|[1-9])");
        Grok grok = new Grok(bank, "%{MONTHDAY:greatday}", logger::warn);
        assertThat(grok.captures("nomatch"), nullValue());
    }

    public void testMultipleNamedCapturesWithSameName() {
        Map<String, String> bank = new HashMap<>();
        bank.put("SINGLEDIGIT", "[0-9]");
        Grok grok = new Grok(bank, "%{SINGLEDIGIT:num}%{SINGLEDIGIT:num}", logger::warn);
        assertCaptureConfig(grok, Map.of("num", STRING));

        Map<String, Object> expected = new HashMap<>();
        expected.put("num", "1");
        assertThat(grok.captures("12"), equalTo(expected));
    }

    public void testExponentialExpressions() {
        testExponentialExpressions(false);
        testExponentialExpressions(true);
    }

    private void testExponentialExpressions(boolean ecsCompatibility) {
        AtomicBoolean run = new AtomicBoolean(true); // to avoid a lingering thread when test has completed

        String grokPattern = "Bonsuche mit folgender Anfrage: Belegart->\\[%{WORD:param2},(?<param5>(\\s*%{NOTSPACE})*)\\] " +
            "Zustand->ABGESCHLOSSEN Kassennummer->%{WORD:param9} Bonnummer->%{WORD:param10} Datum->%{DATESTAMP_OTHER:param11}";
        String logLine = "Bonsuche mit folgender Anfrage: Belegart->[EINGESCHRAENKTER_VERKAUF, VERKAUF, NACHERFASSUNG] " +
            "Zustand->ABGESCHLOSSEN Kassennummer->2 Bonnummer->6362 Datum->Mon Jan 08 00:00:00 UTC 2018";
        BiConsumer<Long, Runnable> scheduler = (delay, command) -> {
            try {
                Thread.sleep(delay);
            } catch (InterruptedException e) {
                throw new AssertionError(e);
            }
            Thread t = new Thread(() -> {
                if (run.get()) {
                    command.run();
                }
            });
            t.start();
        };
        Grok grok = new Grok(
            Grok.getBuiltinPatterns(ecsCompatibility),
            grokPattern,
            MatcherWatchdog.newInstance(10, 200, System::currentTimeMillis, scheduler),
            logger::warn
        );
        Exception e = expectThrows(RuntimeException.class, () -> grok.captures(logLine));
        run.set(false);
        assertThat(e.getMessage(), equalTo("grok pattern matching was interrupted after [200] ms"));
    }

    public void testAtInFieldName() {
        assertGrokedField("@metadata");
    }

    public void assertNonAsciiLetterInFieldName() {
        assertGrokedField("metädata");
    }

    public void assertSquareBracketInFieldName() {
        assertGrokedField("metadat[a]");
        assertGrokedField("metad[a]ta");
        assertGrokedField("[m]etadata");
    }

    public void testUnderscoreInFieldName() {
        assertGrokedField("meta_data");
    }

    public void testDotInFieldName() {
        assertGrokedField("meta.data");
    }

    public void testMinusInFieldName() {
        assertGrokedField("meta-data");
    }

    public void testAlphanumericFieldName() {
        assertGrokedField(randomAlphaOfLengthBetween(1, 5));
        assertGrokedField(randomAlphaOfLengthBetween(1, 5) + randomIntBetween(0, 100));
        assertGrokedField(randomIntBetween(0, 100) + randomAlphaOfLengthBetween(1, 5));
        assertGrokedField(String.valueOf(randomIntBetween(0, 100)));
    }

    public void testUnsupportedBracketsInFieldName() {
        testUnsupportedBracketsInFieldName(false);
        testUnsupportedBracketsInFieldName(true);
    }

    private void testUnsupportedBracketsInFieldName(boolean ecsCompatibility) {
        Grok grok = new Grok(Grok.getBuiltinPatterns(ecsCompatibility), "%{WORD:unsuppo(r)ted}", logger::warn);
        Map<String, Object> matches = grok.captures("line");
        assertNull(matches);
    }

    public void testJavaClassPatternWithUnderscore() {
        testJavaClassPatternWithUnderscore(false);
        testJavaClassPatternWithUnderscore(true);
    }

    private void testJavaClassPatternWithUnderscore(boolean ecsCompatibility) {
        Grok grok = new Grok(Grok.getBuiltinPatterns(ecsCompatibility), "%{JAVACLASS}", logger::warn);
        assertThat(grok.match("Test_Class.class"), is(true));
    }

    public void testJavaFilePatternWithSpaces() {
        testJavaFilePatternWithSpaces(false);
        testJavaFilePatternWithSpaces(true);
    }

    private void testJavaFilePatternWithSpaces(boolean ecsCompatibility) {
        Grok grok = new Grok(Grok.getBuiltinPatterns(ecsCompatibility), "%{JAVAFILE}", logger::warn);
        assertThat(grok.match("Test Class.java"), is(true));
    }

    public void testLogCallBack() {
        testLogCallBack(false);
        testLogCallBack(true);
    }

    private void testLogCallBack(boolean ecsCompatibility) {
        AtomicReference<String> message = new AtomicReference<>();
        Grok grok = new Grok(Grok.getBuiltinPatterns(ecsCompatibility), ".*\\[.*%{SPACE}*\\].*", message::set);
        grok.match("[foo]");
        //this message comes from Joni, so updates to Joni may change the expectation
        assertThat(message.get(), containsString("regular expression has redundant nested repeat operator"));
    }

    private void assertGrokedField(String fieldName) {
        String line = "foo";
        // test both with and without ECS compatibility
        for (boolean ecsCompatibility : new boolean[]{false, true}) {
            Grok grok = new Grok(Grok.getBuiltinPatterns(ecsCompatibility), "%{WORD:" + fieldName + "}", logger::warn);
            Map<String, Object> matches = grok.captures(line);
            assertEquals(line, matches.get(fieldName));
        }
    }

    private void assertCaptureConfig(Grok grok, Map<String, GrokCaptureType> nameToType) {
        assertCaptureConfig(grok, nameToType, List.of());
    }

    private void assertCaptureConfig(Grok grok, Map<String, GrokCaptureType> nameToType, List<String> acceptedDuplicates) {
        Map<String, GrokCaptureType> fromGrok = new TreeMap<>();
        for (GrokCaptureConfig config : grok.captureConfig()) {
            Object old = fromGrok.put(config.name(), config.type());
            if (acceptedDuplicates.contains(config.name()) == false) {
                assertThat("duplicates not allowed", old, nullValue());
            }
        }
        assertThat(fromGrok, equalTo(new TreeMap<>(nameToType)));
    }

    private GrokCaptureConfig namedConfig(Grok grok, String name) {
        return grok.captureConfig().stream().filter(i -> i.name().equals(name)).findFirst().get();
    }

    private boolean specificCapture(Grok grok, String str, GrokCaptureExtracter extracter) {
        byte[] utf8 = str.getBytes(StandardCharsets.UTF_8);
        return grok.match(utf8, 0, utf8.length, extracter);
    }

    private abstract class ThrowingNativeExtracterMap implements NativeExtracterMap<GrokCaptureExtracter> {
        @Override
        public GrokCaptureExtracter forString(Function<Consumer<String>, GrokCaptureExtracter> buildExtracter) {
            throw new IllegalArgumentException();
        }

        @Override
        public GrokCaptureExtracter forInt(Function<IntConsumer, GrokCaptureExtracter> buildExtracter) {
            throw new IllegalArgumentException();
        }

        @Override
        public GrokCaptureExtracter forLong(Function<LongConsumer, GrokCaptureExtracter> buildExtracter) {
            throw new IllegalArgumentException();
        }

        @Override
        public GrokCaptureExtracter forFloat(Function<FloatConsumer, GrokCaptureExtracter> buildExtracter) {
            throw new IllegalArgumentException();
        }

        @Override
        public GrokCaptureExtracter forDouble(Function<DoubleConsumer, GrokCaptureExtracter> buildExtracter) {
            throw new IllegalArgumentException();
        }

        @Override
        public GrokCaptureExtracter forBoolean(Function<Consumer<Boolean>, GrokCaptureExtracter> buildExtracter) {
            throw new IllegalArgumentException();
        }
    }
}

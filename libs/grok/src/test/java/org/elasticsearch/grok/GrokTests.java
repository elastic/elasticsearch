/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.grok;

import org.elasticsearch.grok.GrokCaptureConfig.NativeExtracterMap;
import org.elasticsearch.test.ESTestCase;

import java.nio.charset.StandardCharsets;
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
        Grok grok = new Grok(Grok.BUILTIN_PATTERNS, "value", logger::warn);
        assertThat(grok.captures("value"), equalTo(Map.of()));
        assertThat(grok.captures("prefix_value"), equalTo(Map.of()));
        assertThat(grok.captures("no_match"), nullValue());
    }

    public void testCaputuresBytes() {
        Grok grok = new Grok(Grok.BUILTIN_PATTERNS, "%{NUMBER:n:int}", logger::warn);
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
        String line = "Mar 16 00:01:25 evita postfix/smtpd[1713]: connect from camomile.cloud9.net[168.100.1.3]";
        Grok grok = new Grok(Grok.BUILTIN_PATTERNS, "%{SYSLOGLINE}", logger::warn);
        assertCaptureConfig(
            grok,
            Map.ofEntries(
                Map.entry("facility", STRING),
                Map.entry("logsource", STRING),
                Map.entry("message", STRING),
                Map.entry("pid", STRING),
                Map.entry("priority", STRING),
                Map.entry("program", STRING),
                Map.entry("timestamp", STRING),
                Map.entry("timestamp8601", STRING)
            )
        );
        Map<String, Object> matches = grok.captures(line);
        assertEquals("evita", matches.get("logsource"));
        assertEquals("Mar 16 00:01:25", matches.get("timestamp"));
        assertEquals("connect from camomile.cloud9.net[168.100.1.3]", matches.get("message"));
        assertEquals("postfix/smtpd", matches.get("program"));
        assertEquals("1713", matches.get("pid"));

        String[] logsource = new String[1];
        GrokCaptureExtracter logsourceExtracter = namedConfig(grok, "logsource").nativeExtracter(new ThrowingNativeExtracterMap() {
            @Override
            public GrokCaptureExtracter forString(Function<Consumer<String>, GrokCaptureExtracter> buildExtracter) {
                return buildExtracter.apply(str -> logsource[0] = str);
            }
        });
        assertThat(specificCapture(grok, line, logsourceExtracter), is(true));
        assertThat(logsource[0], equalTo("evita"));
    }

    public void testSyslog5424Line() {
        String line = "<191>1 2009-06-30T18:30:00+02:00 paxton.local grokdebug 4123 - [id1 foo=\\\"bar\\\"][id2 baz=\\\"something\\\"] " +
                "Hello, syslog.";
        Grok grok = new Grok(Grok.BUILTIN_PATTERNS, "%{SYSLOG5424LINE}", logger::warn);
        assertCaptureConfig(
            grok,
            Map.ofEntries(
                Map.entry("syslog5424_app", STRING),
                Map.entry("syslog5424_host", STRING),
                Map.entry("syslog5424_msg", STRING),
                Map.entry("syslog5424_msgid", STRING),
                Map.entry("syslog5424_pri", STRING),
                Map.entry("syslog5424_proc", STRING),
                Map.entry("syslog5424_sd", STRING),
                Map.entry("syslog5424_ts", STRING),
                Map.entry("syslog5424_ver", STRING)
            )
        );
        Map<String, Object> matches = grok.captures(line);
        assertEquals("191", matches.get("syslog5424_pri"));
        assertEquals("1", matches.get("syslog5424_ver"));
        assertEquals("2009-06-30T18:30:00+02:00", matches.get("syslog5424_ts"));
        assertEquals("paxton.local", matches.get("syslog5424_host"));
        assertEquals("grokdebug", matches.get("syslog5424_app"));
        assertEquals("4123", matches.get("syslog5424_proc"));
        assertEquals(null, matches.get("syslog5424_msgid"));
        assertEquals("[id1 foo=\\\"bar\\\"][id2 baz=\\\"something\\\"]", matches.get("syslog5424_sd"));
        assertEquals("Hello, syslog.", matches.get("syslog5424_msg"));
    }

    public void testDatePattern() {
        String line = "fancy 12-12-12 12:12:12";
        Grok grok = new Grok(Grok.BUILTIN_PATTERNS, "(?<timestamp>%{DATE_EU} %{TIME})", logger::warn);
        assertCaptureConfig(grok, Map.of("timestamp", STRING));
        Map<String, Object> matches = grok.captures(line);
        assertEquals("12-12-12 12:12:12", matches.get("timestamp"));
    }

    public void testNilCoercedValues() {
        Grok grok = new Grok(Grok.BUILTIN_PATTERNS, "test (N/A|%{BASE10NUM:duration:float}ms)", logger::warn);
        assertCaptureConfig(grok, Map.of("duration", FLOAT));
        Map<String, Object> matches = grok.captures("test 28.4ms");
        assertEquals(28.4f, matches.get("duration"));
        matches = grok.captures("test N/A");
        assertEquals(null, matches.get("duration"));
    }

    public void testNilWithNoCoercion() {
        Grok grok = new Grok(Grok.BUILTIN_PATTERNS, "test (N/A|%{BASE10NUM:duration}ms)", logger::warn);
        assertCaptureConfig(grok, Map.of("duration", STRING));
        Map<String, Object> matches = grok.captures("test 28.4ms");
        assertEquals("28.4", matches.get("duration"));
        matches = grok.captures("test N/A");
        assertEquals(null, matches.get("duration"));
    }

    public void testUnicodeSyslog() {
        Grok grok = new Grok(Grok.BUILTIN_PATTERNS, "<%{POSINT:syslog_pri}>%{SPACE}%{SYSLOGTIMESTAMP:syslog_timestamp} " +
                "%{SYSLOGHOST:syslog_hostname} %{PROG:syslog_program}(:?)(?:\\[%{GREEDYDATA:syslog_pid}\\])?(:?) " +
                "%{GREEDYDATA:syslog_message}", logger::warn);
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
        Grok grok = new Grok(Grok.BUILTIN_PATTERNS, "%{DATE_EU:stimestamp}", logger::warn);
        assertCaptureConfig(grok, Map.of("stimestamp", STRING));
        Map<String, Object> matches = grok.captures("11/01/01");
        assertThat(matches.get("stimestamp"), equalTo("11/01/01"));
    }

    public void testWithOniguramaNamedCaptures() {
        Grok grok = new Grok(Grok.BUILTIN_PATTERNS, "(?<foo>\\w+)", logger::warn);
        assertCaptureConfig(grok, Map.of("foo", STRING));
        Map<String, Object> matches = grok.captures("hello world");
        assertThat(matches.get("foo"), equalTo("hello"));
    }

    public void testISO8601() {
        Grok grok = new Grok(Grok.BUILTIN_PATTERNS, "^%{TIMESTAMP_ISO8601}$", logger::warn);
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
        Grok grok = new Grok(Grok.BUILTIN_PATTERNS, "^%{TIMESTAMP_ISO8601}$", logger::warn);
        assertCaptureConfig(grok, Map.of());
        List<String> timeMessages = Arrays.asList(
                "2001-13-01T00:00:00", // invalid month
                "2001-00-01T00:00:00", // invalid month
                "2001-01-00T00:00:00", // invalid day
                "2001-01-32T00:00:00", // invalid day
                "2001-01-aT00:00:00", // invalid day
                "2001-01-1aT00:00:00", // invalid day
                "2001-01-01Ta0:00:00", // invalid hour
                "2001-01-01T0:00:00", // invalid hour
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
        for (String msg : timeMessages) {
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
        String pattern = "%{WORD:name}=%{WORD:status:boolean}";
        Grok g = new Grok(Grok.BUILTIN_PATTERNS, pattern, logger::warn);
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
        String logLine = "31.184.238.164 - - [24/Jul/2014:05:35:37 +0530] \"GET /logs/access.log HTTP/1.0\" 200 69849 " +
                "\"http://8rursodiol.enjin.com\" \"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) " +
                "Chrome/30.0.1599.12785 YaBrowser/13.12.1599.12785 Safari/537.36\" \"www.dlwindianrailways.com\"";
        Grok grok = new Grok(Grok.BUILTIN_PATTERNS, "%{COMBINEDAPACHELOG}", logger::warn);
        assertCaptureConfig(
            grok,
            Map.ofEntries(
                Map.entry("agent", STRING),
                Map.entry("auth", STRING),
                Map.entry("bytes", STRING),
                Map.entry("clientip", STRING),
                Map.entry("httpversion", STRING),
                Map.entry("ident", STRING),
                Map.entry("rawrequest", STRING),
                Map.entry("referrer", STRING),
                Map.entry("request", STRING),
                Map.entry("response", STRING),
                Map.entry("timestamp", STRING),
                Map.entry("verb", STRING)
            )
        );
        Map<String, Object> matches = grok.captures(logLine);

        assertEquals("31.184.238.164", matches.get("clientip"));
        assertEquals("-", matches.get("ident"));
        assertEquals("-", matches.get("auth"));
        assertEquals("24/Jul/2014:05:35:37 +0530", matches.get("timestamp"));
        assertEquals("GET", matches.get("verb"));
        assertEquals("/logs/access.log", matches.get("request"));
        assertEquals("1.0", matches.get("httpversion"));
        assertEquals("200", matches.get("response"));
        assertEquals("69849", matches.get("bytes"));
        assertEquals("\"http://8rursodiol.enjin.com\"", matches.get("referrer"));
        assertEquals(null, matches.get("port"));
        assertEquals("\"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/30.0.1599.12785 " +
                "YaBrowser/13.12.1599.12785 Safari/537.36\"", matches.get("agent"));
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
        Grok grok = new Grok(Grok.BUILTIN_PATTERNS, grokPattern, MatcherWatchdog.newInstance(10, 200, System::currentTimeMillis, scheduler),
            logger::warn);
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
        Grok grok = new Grok(Grok.BUILTIN_PATTERNS, "%{WORD:unsuppo(r)ted}", logger::warn);
        Map<String, Object> matches = grok.captures("line");
        assertNull(matches);
    }

    public void testJavaClassPatternWithUnderscore() {
        Grok grok = new Grok(Grok.BUILTIN_PATTERNS, "%{JAVACLASS}", logger::warn);
        assertThat(grok.match("Test_Class.class"), is(true));
    }

    public void testJavaFilePatternWithSpaces() {
        Grok grok = new Grok(Grok.BUILTIN_PATTERNS, "%{JAVAFILE}", logger::warn);
        assertThat(grok.match("Test Class.java"), is(true));
    }

    public void testLogCallBack(){
        AtomicReference<String> message = new AtomicReference<>();
        Grok grok = new Grok(Grok.BUILTIN_PATTERNS, ".*\\[.*%{SPACE}*\\].*", message::set);
        grok.match("[foo]");
        //this message comes from Joni, so updates to Joni may change the expectation
        assertThat(message.get(), containsString("regular expression has redundant nested repeat operator"));
    }

    private void assertGrokedField(String fieldName) {
        String line = "foo";
        Grok grok = new Grok(Grok.BUILTIN_PATTERNS, "%{WORD:" + fieldName + "}", logger::warn);
        Map<String, Object> matches = grok.captures(line);
        assertEquals(line, matches.get(fieldName));
    }

    private void assertCaptureConfig(Grok grok, Map<String, GrokCaptureType> nameToType) {
        Map<String, GrokCaptureType> fromGrok = new TreeMap<>();
        for (GrokCaptureConfig config : grok.captureConfig()) {
            Object old = fromGrok.put(config.name(), config.type());
            assertThat("duplicates not allowed", old, nullValue());
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

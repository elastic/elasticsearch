/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.common.logging;

import com.carrotsearch.randomizedtesting.generators.CodepointSetGenerator;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.hamcrest.RegexMatcher;
import org.hamcrest.core.IsSame;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;

import static org.elasticsearch.common.logging.HeaderWarning.WARNING_HEADER_PATTERN;
import static org.elasticsearch.test.hamcrest.RegexMatcher.matches;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;

/**
 * Tests {@link HeaderWarning}
 */
public class HeaderWarningTests extends ESTestCase {

    private static final RegexMatcher warningValueMatcher = matches(WARNING_HEADER_PATTERN.pattern());

    private final HeaderWarning logger = new HeaderWarning();

    @Override
    protected boolean enableWarningsCheck() {
        // this is a low level test for the deprecation logger, setup and checks are done manually
        return false;
    }

    public void testAddsHeaderWithThreadContext() throws IOException {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        final Set<ThreadContext> threadContexts = Collections.singleton(threadContext);

        final String param = randomAlphaOfLengthBetween(1, 5);
        HeaderWarning.addWarning(threadContexts, "A simple message [{}]", param);

        final Map<String, List<String>> responseHeaders = threadContext.getResponseHeaders();

        assertThat(responseHeaders.size(), equalTo(1));
        final List<String> responses = responseHeaders.get("Warning");
        assertThat(responses, hasSize(1));
        assertThat(responses.get(0), warningValueMatcher);
        assertThat(responses.get(0), containsString("\"A simple message [" + param + "]\""));
    }

    public void testContainingNewline() throws IOException {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        final Set<ThreadContext> threadContexts = Collections.singleton(threadContext);

        HeaderWarning.addWarning(threadContexts, "this message contains a newline\n");

        final Map<String, List<String>> responseHeaders = threadContext.getResponseHeaders();

        assertThat(responseHeaders.size(), equalTo(1));
        final List<String> responses = responseHeaders.get("Warning");
        assertThat(responses, hasSize(1));
        assertThat(responses.get(0), warningValueMatcher);
        assertThat(responses.get(0), containsString("\"this message contains a newline%0A\""));
    }

    public void testSurrogatePair() throws IOException {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        final Set<ThreadContext> threadContexts = Collections.singleton(threadContext);

        HeaderWarning.addWarning(threadContexts, "this message contains a surrogate pair 😱");

        final Map<String, List<String>> responseHeaders = threadContext.getResponseHeaders();

        assertThat(responseHeaders.size(), equalTo(1));
        final List<String> responses = responseHeaders.get("Warning");
        assertThat(responses, hasSize(1));
        assertThat(responses.get(0), warningValueMatcher);

        // convert UTF-16 to UTF-8 by hand to show the hard-coded constant below is correct
        assertThat("😱", equalTo("\uD83D\uDE31"));
        final int code = 0x10000 + ((0xD83D & 0x3FF) << 10) + (0xDE31 & 0x3FF);
        @SuppressWarnings("PointlessBitwiseExpression")
        final int[] points = new int[] {
            (code >> 18) & 0x07 | 0xF0,
            (code >> 12) & 0x3F | 0x80,
            (code >> 6) & 0x3F | 0x80,
            (code >> 0) & 0x3F | 0x80 };
        final StringBuilder sb = new StringBuilder();
        // noinspection ForLoopReplaceableByForEach
        for (int i = 0; i < points.length; i++) {
            sb.append("%").append(Integer.toString(points[i], 16).toUpperCase(Locale.ROOT));
        }
        assertThat(sb.toString(), equalTo("%F0%9F%98%B1"));
        assertThat(responses.get(0), containsString("\"this message contains a surrogate pair %F0%9F%98%B1\""));
    }

    public void testAddsCombinedHeaderWithThreadContext() throws IOException {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        final Set<ThreadContext> threadContexts = Collections.singleton(threadContext);

        final String param = randomAlphaOfLengthBetween(1, 5);
        HeaderWarning.addWarning(threadContexts, "A simple message [{}]", param);
        final String second = randomAlphaOfLengthBetween(1, 10);
        HeaderWarning.addWarning(threadContexts, second);

        final Map<String, List<String>> responseHeaders = threadContext.getResponseHeaders();

        assertEquals(1, responseHeaders.size());

        final List<String> responses = responseHeaders.get("Warning");

        assertEquals(2, responses.size());
        assertThat(responses.get(0), warningValueMatcher);
        assertThat(responses.get(0), containsString("\"A simple message [" + param + "]\""));
        assertThat(responses.get(1), warningValueMatcher);
        assertThat(responses.get(1), containsString("\"" + second + "\""));
    }

    public void testCanRemoveThreadContext() throws IOException {
        final String expected = "testCanRemoveThreadContext";
        final String unexpected = "testCannotRemoveThreadContext";

        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        HeaderWarning.setThreadContext(threadContext);
        /*"testCanRemoveThreadContext_key1",*/
        HeaderWarning.addWarning(HeaderWarning.THREAD_CONTEXT, expected);

        {
            final Map<String, List<String>> responseHeaders = threadContext.getResponseHeaders();
            final List<String> responses = responseHeaders.get("Warning");

            assertThat(responses, hasSize(1));
            assertThat(responses.get(0), warningValueMatcher);
            assertThat(responses.get(0), containsString(expected));
        }

        HeaderWarning.removeThreadContext(threadContext);
        /*"testCanRemoveThreadContext_key2", */
        HeaderWarning.addWarning(HeaderWarning.THREAD_CONTEXT, unexpected);

        {
            final Map<String, List<String>> responseHeaders = threadContext.getResponseHeaders();
            final List<String> responses = responseHeaders.get("Warning");

            assertThat(responses, hasSize(1));
            assertThat(responses.get(0), warningValueMatcher);
            assertThat(responses.get(0), containsString(expected));
            assertThat(responses.get(0), not(containsString(unexpected)));
        }
    }

    public void testSafeWithoutThreadContext() {
        HeaderWarning.addWarning(Collections.emptySet(), "Ignored");
    }

    public void testFailsWithoutThreadContextSet() {
        expectThrows(NullPointerException.class, () -> HeaderWarning.addWarning((Set<ThreadContext>) null, "Does not explode"));
    }

    public void testFailsWhenDoubleSettingSameThreadContext() throws IOException {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        HeaderWarning.setThreadContext(threadContext);

        try {
            expectThrows(IllegalStateException.class, () -> HeaderWarning.setThreadContext(threadContext));
        } finally {
            // cleanup after ourselves
            HeaderWarning.removeThreadContext(threadContext);
        }
    }

    public void testFailsWhenRemovingUnknownThreadContext() throws IOException {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        expectThrows(IllegalStateException.class, () -> HeaderWarning.removeThreadContext(threadContext));
    }

    public void testWarningValueFromWarningHeader() {
        final String s = randomAlphaOfLength(16);
        final String first = HeaderWarning.formatWarning(s);
        assertThat(HeaderWarning.extractWarningValueFromWarningHeader(first, false), equalTo(s));

        final String withPos = "[context][1:11] Blah blah blah";
        final String formatted = HeaderWarning.formatWarning(withPos);
        assertThat(HeaderWarning.extractWarningValueFromWarningHeader(formatted, true), equalTo("Blah blah blah"));

        final String withNegativePos = "[context][-1:-1] Blah blah blah";
        assertThat(
            HeaderWarning.extractWarningValueFromWarningHeader(HeaderWarning.formatWarning(withNegativePos), true),
            equalTo("Blah blah blah")
        );
    }

    public void testEscapeBackslashesAndQuotes() {
        assertThat(HeaderWarning.escapeBackslashesAndQuotes("\\"), equalTo("\\\\"));
        assertThat(HeaderWarning.escapeBackslashesAndQuotes("\""), equalTo("\\\""));
        assertThat(HeaderWarning.escapeBackslashesAndQuotes("\\\""), equalTo("\\\\\\\""));
        assertThat(HeaderWarning.escapeBackslashesAndQuotes("\"foo\\bar\""), equalTo("\\\"foo\\\\bar\\\""));
        // test that characters other than '\' and '"' are left unchanged
        String chars = "\t !" + range(0x23, 0x24) + range(0x26, 0x5b) + range(0x5d, 0x73) + range(0x80, 0xff);
        final String s = new CodepointSetGenerator(chars.toCharArray()).ofCodePointsLength(random(), 16, 16);
        assertThat(HeaderWarning.escapeBackslashesAndQuotes(s), equalTo(s));
    }

    public void testEncode() {
        assertThat(HeaderWarning.encode("\n"), equalTo("%0A"));
        assertThat(HeaderWarning.encode("😱"), equalTo("%F0%9F%98%B1"));
        assertThat(HeaderWarning.encode("福島深雪"), equalTo("%E7%A6%8F%E5%B3%B6%E6%B7%B1%E9%9B%AA"));
        assertThat(HeaderWarning.encode("100%\n"), equalTo("100%25%0A"));
        // test that valid characters are left unchanged
        String chars = "\t !" + range(0x23, 0x24) + range(0x26, 0x5b) + range(0x5d, 0x73) + range(0x80, 0xff) + '\\' + '"';
        final String s = new CodepointSetGenerator(chars.toCharArray()).ofCodePointsLength(random(), 16, 16);
        assertThat(HeaderWarning.encode(s), equalTo(s));
        // when no encoding is needed, the original string is returned (optimization)
        assertThat(HeaderWarning.encode(s), IsSame.sameInstance(s));
    }

    public void testWarningHeaderCountSetting() throws IOException {
        // Test that the number of warning headers don't exceed 'http.max_warning_header_count'
        final int maxWarningHeaderCount = 2;
        Settings settings = Settings.builder().put("http.max_warning_header_count", maxWarningHeaderCount).build();
        ThreadContext threadContext = new ThreadContext(settings);
        final Set<ThreadContext> threadContexts = Collections.singleton(threadContext);
        // try to log three warning messages
        HeaderWarning.addWarning(threadContexts, "A simple message 1");
        HeaderWarning.addWarning(threadContexts, "A simple message 2");
        HeaderWarning.addWarning(threadContexts, "A simple message 3");
        final Map<String, List<String>> responseHeaders = threadContext.getResponseHeaders();
        final List<String> responses = responseHeaders.get("Warning");

        assertEquals(maxWarningHeaderCount, responses.size());
        assertThat(responses.get(0), warningValueMatcher);
        assertThat(responses.get(0), containsString("\"A simple message 1"));
        assertThat(responses.get(1), warningValueMatcher);
        assertThat(responses.get(1), containsString("\"A simple message 2"));
    }

    public void testWarningHeaderSizeSetting() throws IOException {
        // Test that the size of warning headers don't exceed 'http.max_warning_header_size'
        Settings settings = Settings.builder().put("http.max_warning_header_size", "1Kb").build();

        byte[] arr = new byte[300];
        String message1 = new String(arr, StandardCharsets.UTF_8) + "1";
        String message2 = new String(arr, StandardCharsets.UTF_8) + "2";
        String message3 = new String(arr, StandardCharsets.UTF_8) + "3";

        ThreadContext threadContext = new ThreadContext(settings);
        final Set<ThreadContext> threadContexts = Collections.singleton(threadContext);
        // try to log three warning messages
        HeaderWarning.addWarning(threadContexts, message1);
        HeaderWarning.addWarning(threadContexts, message2);
        HeaderWarning.addWarning(threadContexts, message3);
        final Map<String, List<String>> responseHeaders = threadContext.getResponseHeaders();
        final List<String> responses = responseHeaders.get("Warning");

        long warningHeadersSize = 0L;
        for (String response : responses) {
            warningHeadersSize += "Warning".getBytes(StandardCharsets.UTF_8).length + response.getBytes(StandardCharsets.UTF_8).length;
        }
        // assert that the size of all warning headers is less or equal to 1Kb
        assertTrue(warningHeadersSize <= 1024);
    }

    private String range(int lowerInclusive, int upperInclusive) {
        return IntStream.range(lowerInclusive, upperInclusive + 1)
            .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
            .toString();
    }

    public void testAddWarningNonDefaultLogLevel() {
        final int maxWarningHeaderCount = 2;
        Settings settings = Settings.builder().put("http.max_warning_header_count", maxWarningHeaderCount).build();
        ThreadContext threadContext = new ThreadContext(settings);
        final Set<ThreadContext> threadContexts = Collections.singleton(threadContext);
        HeaderWarning.addWarning(threadContexts, "A simple message 1");
        final Map<String, List<String>> responseHeaders = threadContext.getResponseHeaders();

        assertThat(responseHeaders.size(), equalTo(1));
        final List<String> responses = responseHeaders.get("Warning");
        assertThat(responses, hasSize(1));
        assertThat(responses.get(0), warningValueMatcher);
        assertThat(responses.get(0), containsString("\"A simple message 1\""));
        assertThat(responses.get(0), containsString(Integer.toString(299)));
    }

    // Reproduces https://github.com/elastic/elasticsearch/issues/95972
    public void testAddComplexWarning() {
        final int maxWarningHeaderCount = 2;
        Settings settings = Settings.builder().put("http.max_warning_header_count", maxWarningHeaderCount).build();
        ThreadContext threadContext = new ThreadContext(settings);
        final Set<ThreadContext> threadContexts = Collections.singleton(threadContext);
        HeaderWarning.addWarning(
            threadContexts,
            "legacy template [global] has index patterns [*] matching patterns from existing composable templates "
                + "[.deprecation-indexing-template,.fleet-file-data,.fleet-files,.ml-anomalies-,.ml-notifications-000002,.ml-state,"
                + ".ml-stats,.monitoring-beats-mb,.monitoring-ent-search-mb,.monitoring-es-mb,.monitoring-kibana-mb,"
                + ".monitoring-logstash-mb,.profiling-ilm-lock,.slm-history,.watch-history-16,behavioral_analytics-events-default,"
                + "ilm-history,logs,metrics,profiling-events,profiling-executables,profiling-metrics,profiling-returnpads-private,"
                + "profiling-sq-executables,profiling-sq-leafframes,profiling-stackframes,profiling-stacktraces,"
                + "profiling-symbols,synthetics] with patterns (.deprecation-indexing-template => [.logs-deprecation.*],"
                + ".fleet-file-data => [.fleet-file-data-*-*],.fleet-files => [.fleet-files-*-*],.ml-anomalies- => [.ml-anomalies-*],"
                + ".ml-notifications-000002 => [.ml-notifications-000002],.ml-state => [.ml-state*],.ml-stats => [.ml-stats-*],"
                + ".monitoring-beats-mb => [.monitoring-beats-8-*],.monitoring-ent-search-mb => [.monitoring-ent-search-8-*],"
                + ".monitoring-es-mb => [.monitoring-es-8-*],.monitoring-kibana-mb => [.monitoring-kibana-8-*],"
                + ".monitoring-logstash-mb => [.monitoring-logstash-8-*],.profiling-ilm-lock => [.profiling-ilm-lock*],"
                + ".slm-history => [.slm-history-5*],.watch-history-16 => [.watcher-history-16*],"
                + "behavioral_analytics-events-default => [behavioral_analytics-events-*],ilm-history => [ilm-history-5*],"
                + "logs => [logs-*-*],metrics => [metrics-*-*],profiling-events => [profiling-events*],profiling-executables => "
                + "[profiling-executables*],profiling-metrics => [profiling-metrics*],profiling-returnpads-private => "
                + "[.profiling-returnpads-private*],profiling-sq-executables => [.profiling-sq-executables*],"
                + "profiling-sq-leafframes => [.profiling-sq-leafframes*],profiling-stackframes => [profiling-stackframes*],"
                + "profiling-stacktraces => [profiling-stacktraces*],profiling-symbols => [.profiling-symbols*],synthetics => "
                + "[synthetics-*-*]); this template [global] may be ignored in favor of a composable template at index creation time"
        );
        final Map<String, List<String>> responseHeaders = threadContext.getResponseHeaders();

        assertThat(responseHeaders.size(), equalTo(1));
        final List<String> responses = responseHeaders.get("Warning");
        assertThat(responses, hasSize(1));
        // TODO 95972: As this uses the same regex as the assertion in HeaderWarning#addWarning() this also causes a StackOverflowError
        // assertThat(responses.get(0), warningValueMatcher);
        assertThat(responses.get(0), containsString("\"legacy template [global] has index patterns"));
        assertThat(responses.get(0), containsString(Integer.toString(299)));
    }

    public void testHeaderWarningValidation() {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        final Set<ThreadContext> threadContexts = Collections.singleton(threadContext);

        HeaderWarning.addWarning(threadContexts, allAllowedChars());

        // LoggerMessageFormat.format makes sure all not allowed chars are escaped
        HeaderWarning.addWarning(threadContexts, "\"");
        HeaderWarning.addWarning(threadContexts, "\\");
        HeaderWarning.addWarning(threadContexts, allNotAllowedChars());
    }

    private String allNotAllowedChars() {
        StringBuilder chars = new StringBuilder();
        for (char c = 0; c < 256; c++) {
            if (c < '\t' || ('\t' < c && c < 0x20) || c == 0x7f) {
                chars.append(c);
            }
        }
        return chars.toString();
    }

    private static String allAllowedChars() {
        StringBuilder allPossibleChars = new StringBuilder();
        allPossibleChars.append('\t');
        allPossibleChars.append(' ');
        allPossibleChars.append('!');
        for (int i = 0x23; i <= 0x5b; i++) {
            allPossibleChars.append((char) i);
        }
        for (int i = 0x5d; i <= 0x7e; i++) {
            allPossibleChars.append((char) i);
        }
        for (int i = 0x80; i <= 0xff; i++) {
            allPossibleChars.append((char) i);
        }
        allPossibleChars.append("\\");
        allPossibleChars.append("\\\"");
        return allPossibleChars.toString();
    }
}

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.common.logging;

import com.carrotsearch.randomizedtesting.generators.CodepointSetGenerator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.simple.SimpleLoggerContext;
import org.apache.logging.log4j.simple.SimpleLoggerContextFactory;
import org.apache.logging.log4j.spi.ExtendedLogger;
import org.apache.logging.log4j.spi.LoggerContext;
import org.apache.logging.log4j.spi.LoggerContextFactory;
import org.elasticsearch.common.SuppressLoggerChecks;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.hamcrest.RegexMatcher;
import org.hamcrest.core.IsSame;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.AccessControlContext;
import java.security.AccessController;
import java.security.Permissions;
import java.security.PrivilegedAction;
import java.security.ProtectionDomain;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;

import static org.elasticsearch.common.logging.DeprecationLogger.WARNING_HEADER_PATTERN;
import static org.elasticsearch.test.hamcrest.RegexMatcher.matches;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.core.Is.is;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests {@link DeprecationLogger}
 */
public class DeprecationLoggerTests extends ESTestCase {

    private static final RegexMatcher warningValueMatcher = matches(WARNING_HEADER_PATTERN.pattern());

    private final DeprecationLogger logger = new DeprecationLogger(LogManager.getLogger(getClass()));

    @Override
    protected boolean enableWarningsCheck() {
        //this is a low level test for the deprecation logger, setup and checks are done manually
        return false;
    }

    public void testAddsHeaderWithThreadContext() throws IOException {
        try (ThreadContext threadContext = new ThreadContext(Settings.EMPTY)) {
            final Set<ThreadContext> threadContexts = Collections.singleton(threadContext);

            final String param = randomAlphaOfLengthBetween(1, 5);
            logger.deprecated(threadContexts, "A simple message [{}]", param);

            final Map<String, List<String>> responseHeaders = threadContext.getResponseHeaders();

            assertThat(responseHeaders.size(), equalTo(1));
            final List<String> responses = responseHeaders.get("Warning");
            assertThat(responses, hasSize(1));
            assertThat(responses.get(0), warningValueMatcher);
            assertThat(responses.get(0), containsString("\"A simple message [" + param + "]\""));
        }
    }

    public void testContainingNewline() throws IOException {
        try (ThreadContext threadContext = new ThreadContext(Settings.EMPTY)) {
            final Set<ThreadContext> threadContexts = Collections.singleton(threadContext);

            logger.deprecated(threadContexts, "this message contains a newline\n");

            final Map<String, List<String>> responseHeaders = threadContext.getResponseHeaders();

            assertThat(responseHeaders.size(), equalTo(1));
            final List<String> responses = responseHeaders.get("Warning");
            assertThat(responses, hasSize(1));
            assertThat(responses.get(0), warningValueMatcher);
            assertThat(responses.get(0), containsString("\"this message contains a newline%0A\""));
        }
    }

    public void testSurrogatePair() throws IOException {
        try (ThreadContext threadContext = new ThreadContext(Settings.EMPTY)) {
            final Set<ThreadContext> threadContexts = Collections.singleton(threadContext);

            logger.deprecated(threadContexts, "this message contains a surrogate pair 😱");

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
                    (code >> 0) & 0x3F | 0x80};
            final StringBuilder sb = new StringBuilder();
            // noinspection ForLoopReplaceableByForEach
            for (int i = 0; i < points.length; i++) {
                sb.append("%").append(Integer.toString(points[i], 16).toUpperCase(Locale.ROOT));
            }
            assertThat(sb.toString(), equalTo("%F0%9F%98%B1"));
            assertThat(responses.get(0), containsString("\"this message contains a surrogate pair %F0%9F%98%B1\""));
        }
    }

    public void testAddsCombinedHeaderWithThreadContext() throws IOException {
        try (ThreadContext threadContext = new ThreadContext(Settings.EMPTY)) {
            final Set<ThreadContext> threadContexts = Collections.singleton(threadContext);

            final String param = randomAlphaOfLengthBetween(1, 5);
            logger.deprecated(threadContexts, "A simple message [{}]", param);
            final String second = randomAlphaOfLengthBetween(1, 10);
            logger.deprecated(threadContexts, second);

            final Map<String, List<String>> responseHeaders = threadContext.getResponseHeaders();

            assertEquals(1, responseHeaders.size());

            final List<String> responses = responseHeaders.get("Warning");

            assertEquals(2, responses.size());
            assertThat(responses.get(0), warningValueMatcher);
            assertThat(responses.get(0), containsString("\"A simple message [" + param + "]\""));
            assertThat(responses.get(1), warningValueMatcher);
            assertThat(responses.get(1), containsString("\"" + second + "\""));
        }
    }

    public void testCanRemoveThreadContext() throws IOException {
        final String expected = "testCanRemoveThreadContext";
        final String unexpected = "testCannotRemoveThreadContext";

        try (ThreadContext threadContext = new ThreadContext(Settings.EMPTY)) {
            DeprecationLogger.setThreadContext(threadContext);
            logger.deprecated(expected);

            {
                final Map<String, List<String>> responseHeaders = threadContext.getResponseHeaders();
                final List<String> responses = responseHeaders.get("Warning");

                assertThat(responses, hasSize(1));
                assertThat(responses.get(0), warningValueMatcher);
                assertThat(responses.get(0), containsString(expected));
            }

            DeprecationLogger.removeThreadContext(threadContext);
            logger.deprecated(unexpected);

            {
                final Map<String, List<String>> responseHeaders = threadContext.getResponseHeaders();
                final List<String> responses = responseHeaders.get("Warning");

                assertThat(responses, hasSize(1));
                assertThat(responses.get(0), warningValueMatcher);
                assertThat(responses.get(0), containsString(expected));
                assertThat(responses.get(0), not(containsString(unexpected)));
            }
        }
    }

    public void testIgnoresClosedThreadContext() throws IOException {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        Set<ThreadContext> threadContexts = new HashSet<>(1);

        threadContexts.add(threadContext);

        threadContext.close();

        logger.deprecated(threadContexts, "Ignored logger message");

        assertTrue(threadContexts.contains(threadContext));
    }

    public void testSafeWithoutThreadContext() {
        logger.deprecated(Collections.emptySet(), "Ignored");
    }

    public void testFailsWithoutThreadContextSet() {
        expectThrows(NullPointerException.class, () -> logger.deprecated((Set<ThreadContext>)null, "Does not explode"));
    }

    public void testFailsWhenDoubleSettingSameThreadContext() throws IOException {
        try (ThreadContext threadContext = new ThreadContext(Settings.EMPTY)) {
            DeprecationLogger.setThreadContext(threadContext);

            try {
                expectThrows(IllegalStateException.class, () -> DeprecationLogger.setThreadContext(threadContext));
            } finally {
                // cleanup after ourselves
                DeprecationLogger.removeThreadContext(threadContext);
            }
        }
    }

    public void testFailsWhenRemovingUnknownThreadContext() throws IOException {
        try (ThreadContext threadContext = new ThreadContext(Settings.EMPTY)) {
            expectThrows(IllegalStateException.class, () -> DeprecationLogger.removeThreadContext(threadContext));
        }
    }

    public void testWarningValueFromWarningHeader() throws InterruptedException {
        final String s = randomAlphaOfLength(16);
        final String first = DeprecationLogger.formatWarning(s);
        assertThat(DeprecationLogger.extractWarningValueFromWarningHeader(first), equalTo(s));
    }

    public void testEscapeBackslashesAndQuotes() {
        assertThat(DeprecationLogger.escapeBackslashesAndQuotes("\\"), equalTo("\\\\"));
        assertThat(DeprecationLogger.escapeBackslashesAndQuotes("\""), equalTo("\\\""));
        assertThat(DeprecationLogger.escapeBackslashesAndQuotes("\\\""), equalTo("\\\\\\\""));
        assertThat(DeprecationLogger.escapeBackslashesAndQuotes("\"foo\\bar\""),equalTo("\\\"foo\\\\bar\\\""));
        // test that characters other than '\' and '"' are left unchanged
        String chars = "\t !" + range(0x23, 0x24) + range(0x26, 0x5b) + range(0x5d, 0x73) + range(0x80, 0xff);
        final String s = new CodepointSetGenerator(chars.toCharArray()).ofCodePointsLength(random(), 16, 16);
        assertThat(DeprecationLogger.escapeBackslashesAndQuotes(s), equalTo(s));
    }

    public void testEncode() {
        assertThat(DeprecationLogger.encode("\n"), equalTo("%0A"));
        assertThat(DeprecationLogger.encode("😱"), equalTo("%F0%9F%98%B1"));
        assertThat(DeprecationLogger.encode("福島深雪"), equalTo("%E7%A6%8F%E5%B3%B6%E6%B7%B1%E9%9B%AA"));
        assertThat(DeprecationLogger.encode("100%\n"), equalTo("100%25%0A"));
        // test that valid characters are left unchanged
        String chars = "\t !" + range(0x23, 0x24) + range(0x26, 0x5b) + range(0x5d, 0x73) + range(0x80, 0xff) + '\\' + '"';
        final String s = new CodepointSetGenerator(chars.toCharArray()).ofCodePointsLength(random(), 16, 16);
        assertThat(DeprecationLogger.encode(s), equalTo(s));
        // when no encoding is needed, the original string is returned (optimization)
        assertThat(DeprecationLogger.encode(s), IsSame.sameInstance(s));
    }


    public void testWarningHeaderCountSetting() throws IOException{
        // Test that the number of warning headers don't exceed 'http.max_warning_header_count'
        final int maxWarningHeaderCount = 2;
        Settings settings = Settings.builder()
            .put("http.max_warning_header_count", maxWarningHeaderCount)
            .build();
        try (ThreadContext threadContext = new ThreadContext(settings)) {
            final Set<ThreadContext> threadContexts = Collections.singleton(threadContext);
            // try to log three warning messages
            logger.deprecated(threadContexts, "A simple message 1");
            logger.deprecated(threadContexts, "A simple message 2");
            logger.deprecated(threadContexts, "A simple message 3");
            final Map<String, List<String>> responseHeaders = threadContext.getResponseHeaders();
            final List<String> responses = responseHeaders.get("Warning");

            assertEquals(maxWarningHeaderCount, responses.size());
            assertThat(responses.get(0), warningValueMatcher);
            assertThat(responses.get(0), containsString("\"A simple message 1"));
            assertThat(responses.get(1), warningValueMatcher);
            assertThat(responses.get(1), containsString("\"A simple message 2"));
        }
    }

    public void testWarningHeaderSizeSetting() throws IOException{
        // Test that the size of warning headers don't exceed 'http.max_warning_header_size'
        Settings settings = Settings.builder()
            .put("http.max_warning_header_size", "1Kb")
            .build();

        byte [] arr = new byte[300];
        String message1 = new String(arr, StandardCharsets.UTF_8) + "1";
        String message2 = new String(arr, StandardCharsets.UTF_8) + "2";
        String message3 = new String(arr, StandardCharsets.UTF_8) + "3";

        try (ThreadContext threadContext = new ThreadContext(settings)) {
            final Set<ThreadContext> threadContexts = Collections.singleton(threadContext);
            // try to log three warning messages
            logger.deprecated(threadContexts, message1);
            logger.deprecated(threadContexts, message2);
            logger.deprecated(threadContexts, message3);
            final Map<String, List<String>> responseHeaders = threadContext.getResponseHeaders();
            final List<String> responses = responseHeaders.get("Warning");

            long warningHeadersSize = 0L;
            for (String response : responses){
                warningHeadersSize += "Warning".getBytes(StandardCharsets.UTF_8).length +
                    response.getBytes(StandardCharsets.UTF_8).length;
            }
            // assert that the size of all warning headers is less or equal to 1Kb
            assertTrue(warningHeadersSize <= 1024);
        }
    }
    @SuppressLoggerChecks(reason = "Safe as this is using mockito")
    public void testLogPermissions() {
        AtomicBoolean supplierCalled = new AtomicBoolean(false);

        // mocking the logger used inside DeprecationLogger requires heavy hacking...
        Logger parentLogger = mock(Logger.class);
        when(parentLogger.getName()).thenReturn("logger");
        ExtendedLogger mockLogger = mock(ExtendedLogger.class);
        doAnswer(invocationOnMock -> {
            supplierCalled.set(true);
            createTempDir(); // trigger file permission, like rolling logs would
            return null;
        }).when(mockLogger).warn(new DeprecatedMessage("foo", any()));
        final LoggerContext context = new SimpleLoggerContext() {
            @Override
            public ExtendedLogger getLogger(String name) {
                return mockLogger;
            }
        };

        final LoggerContextFactory originalFactory = LogManager.getFactory();
        try {
            LogManager.setFactory(new SimpleLoggerContextFactory() {
                @Override
                public LoggerContext getContext(String fqcn, ClassLoader loader, Object externalContext, boolean currentContext,
                                                URI configLocation, String name) {
                    return context;
                }
            });
            DeprecationLogger deprecationLogger = new DeprecationLogger(parentLogger);

            AccessControlContext noPermissionsAcc = new AccessControlContext(
                new ProtectionDomain[]{new ProtectionDomain(null, new Permissions())}
            );
            AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
                deprecationLogger.deprecated("foo", "bar");
                return null;
            }, noPermissionsAcc);
            assertThat("supplier called", supplierCalled.get(), is(true));
        } finally {
            LogManager.setFactory(originalFactory);
        }
    }

    private String range(int lowerInclusive, int upperInclusive) {
        return IntStream
                .range(lowerInclusive, upperInclusive + 1)
                .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
                .toString();
    }

}

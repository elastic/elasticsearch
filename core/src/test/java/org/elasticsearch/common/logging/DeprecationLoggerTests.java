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

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;

/**
 * Tests {@link DeprecationLogger}
 */
public class DeprecationLoggerTests extends ESTestCase {

    private final DeprecationLogger logger = new DeprecationLogger(Loggers.getLogger(getClass()));

    @Override
    protected boolean enableWarningsCheck() {
        //this is a low level test for the deprecation logger, setup and checks are done manually
        return false;
    }

    public void testAddsHeaderWithThreadContext() throws IOException {
        String msg = "A simple message [{}]";
        String param = randomAsciiOfLengthBetween(1, 5);
        String formatted = LoggerMessageFormat.format(msg, (Object)param);

        try (ThreadContext threadContext = new ThreadContext(Settings.EMPTY)) {
            Set<ThreadContext> threadContexts = Collections.singleton(threadContext);

            logger.deprecated(threadContexts, msg, param);

            Map<String, List<String>> responseHeaders = threadContext.getResponseHeaders();

            assertEquals(1, responseHeaders.size());
            assertEquals(formatted, responseHeaders.get(DeprecationLogger.WARNING_HEADER).get(0));
        }
    }

    public void testAddsCombinedHeaderWithThreadContext() throws IOException {
        String msg = "A simple message [{}]";
        String param = randomAsciiOfLengthBetween(1, 5);
        String formatted = LoggerMessageFormat.format(msg, (Object)param);
        String formatted2 = randomAsciiOfLengthBetween(1, 10);

        try (ThreadContext threadContext = new ThreadContext(Settings.EMPTY)) {
            Set<ThreadContext> threadContexts = Collections.singleton(threadContext);

            logger.deprecated(threadContexts, msg, param);
            logger.deprecated(threadContexts, formatted2);

            Map<String, List<String>> responseHeaders = threadContext.getResponseHeaders();

            assertEquals(1, responseHeaders.size());

            List<String> responses = responseHeaders.get(DeprecationLogger.WARNING_HEADER);

            assertEquals(2, responses.size());
            assertEquals(formatted, responses.get(0));
            assertEquals(formatted2, responses.get(1));
        }
    }

    public void testCanRemoveThreadContext() throws IOException {
        final String expected = "testCanRemoveThreadContext";
        final String unexpected = "testCannotRemoveThreadContext";

        try (ThreadContext threadContext = new ThreadContext(Settings.EMPTY)) {
            // NOTE: by adding it to the logger, we allow any concurrent test to write to it (from their own threads)
            DeprecationLogger.setThreadContext(threadContext);

            logger.deprecated(expected);

            Map<String, List<String>> responseHeaders = threadContext.getResponseHeaders();
            List<String> responses = responseHeaders.get(DeprecationLogger.WARNING_HEADER);

            // ensure it works (note: concurrent tests may be adding to it, but in different threads, so it should have no impact)
            assertThat(responses, hasSize(atLeast(1)));
            assertThat(responses, hasItem(equalTo(expected)));

            DeprecationLogger.removeThreadContext(threadContext);

            logger.deprecated(unexpected);

            responseHeaders = threadContext.getResponseHeaders();
            responses = responseHeaders.get(DeprecationLogger.WARNING_HEADER);

            assertThat(responses, hasSize(atLeast(1)));
            assertThat(responses, hasItem(expected));
            assertThat(responses, not(hasItem(unexpected)));
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

}

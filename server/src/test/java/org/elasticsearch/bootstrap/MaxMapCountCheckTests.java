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

package org.elasticsearch.bootstrap;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.util.Constants;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.AbstractBootstrapCheckTestCase;
import org.elasticsearch.test.MockLogAppender;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class MaxMapCountCheckTests extends AbstractBootstrapCheckTestCase {

    // initialize as if the max map count is under the limit, tests can override by setting maxMapCount before executing the check
    private final AtomicLong maxMapCount = new AtomicLong(randomIntBetween(1, Math.toIntExact(BootstrapChecks.MaxMapCountCheck.LIMIT) - 1));
    private final BootstrapChecks.MaxMapCountCheck check = new BootstrapChecks.MaxMapCountCheck() {
        @Override
        long getMaxMapCount() {
            return maxMapCount.get();
        }
    };

    private void assertFailure(final BootstrapCheck.BootstrapCheckResult result) {
        assertTrue(result.isFailure());
        assertThat(
                result.getMessage(),
                equalTo(
                        "max virtual memory areas vm.max_map_count [" + maxMapCount.get() + "] is too low, "
                                + "increase to at least [" + BootstrapChecks.MaxMapCountCheck.LIMIT + "]"));
    }

    public void testMaxMapCountCheckBelowLimit() {
        assertFailure(check.check(emptyContext));
    }

    public void testMaxMapCountCheckBelowLimitAndMemoryMapAllowed() {
        /*
         * There are two ways that memory maps are allowed:
         *  - by default
         *  - mmap is explicitly allowed
         * We want to test that if mmap is allowed then the max map count check is enforced.
         */
        final List<Settings> settingsThatAllowMemoryMap = new ArrayList<>();
        settingsThatAllowMemoryMap.add(Settings.EMPTY);
        settingsThatAllowMemoryMap.add(Settings.builder().put("node.store.allow_mmap", true).build());

        for (final Settings settingThatAllowsMemoryMap : settingsThatAllowMemoryMap) {
            assertFailure(check.check(createTestContext(settingThatAllowsMemoryMap, Metadata.EMPTY_METADATA)));
        }
    }

    public void testMaxMapCountCheckNotEnforcedIfMemoryMapNotAllowed() {
        // nothing should happen if current vm.max_map_count is under the limit but mmap is not allowed
        final Settings settings = Settings.builder().put("node.store.allow_mmap", false).build();
        final BootstrapContext context = createTestContext(settings, Metadata.EMPTY_METADATA);
        final BootstrapCheck.BootstrapCheckResult result = check.check(context);
        assertTrue(result.isSuccess());
    }

    public void testMaxMapCountCheckAboveLimit() {
        // nothing should happen if current vm.max_map_count exceeds the limit
        maxMapCount.set(randomIntBetween(Math.toIntExact(BootstrapChecks.MaxMapCountCheck.LIMIT) + 1, Integer.MAX_VALUE));
        final BootstrapCheck.BootstrapCheckResult result = check.check(emptyContext);
        assertTrue(result.isSuccess());
    }

    public void testMaxMapCountCheckMaxMapCountNotAvailable() {
        // nothing should happen if current vm.max_map_count is not available
        maxMapCount.set(-1);
        final BootstrapCheck.BootstrapCheckResult result = check.check(emptyContext);
        assertTrue(result.isSuccess());
    }

    public void testGetMaxMapCountOnLinux() {
        if (Constants.LINUX) {
            final BootstrapChecks.MaxMapCountCheck check = new BootstrapChecks.MaxMapCountCheck();
            assertThat(check.getMaxMapCount(), greaterThan(0L));
        }
    }

    public void testGetMaxMapCount() throws IOException, IllegalAccessException {
        final long procSysVmMaxMapCount = randomIntBetween(1, Integer.MAX_VALUE);
        final BufferedReader reader = mock(BufferedReader.class);
        when(reader.readLine()).thenReturn(Long.toString(procSysVmMaxMapCount));
        final Path procSysVmMaxMapCountPath = PathUtils.get("/proc/sys/vm/max_map_count");
        BootstrapChecks.MaxMapCountCheck check = new BootstrapChecks.MaxMapCountCheck() {
            @Override
            BufferedReader getBufferedReader(Path path) throws IOException {
                assertEquals(path, procSysVmMaxMapCountPath);
                return reader;
            }
        };

        assertThat(check.getMaxMapCount(), equalTo(procSysVmMaxMapCount));
        verify(reader).close();

        {
            reset(reader);
            final IOException ioException = new IOException("fatal");
            when(reader.readLine()).thenThrow(ioException);
            final Logger logger = LogManager.getLogger("testGetMaxMapCountIOException");
            final MockLogAppender appender = new MockLogAppender();
            appender.start();
            appender.addExpectation(
                    new ParameterizedMessageLoggingExpectation(
                            "expected logged I/O exception",
                            "testGetMaxMapCountIOException",
                            Level.WARN,
                            "I/O exception while trying to read [{}]",
                            new Object[] { procSysVmMaxMapCountPath },
                            e -> ioException == e));
            Loggers.addAppender(logger, appender);
            assertThat(check.getMaxMapCount(logger), equalTo(-1L));
            appender.assertAllExpectationsMatched();
            verify(reader).close();
            Loggers.removeAppender(logger, appender);
            appender.stop();
        }

        {
            reset(reader);
            when(reader.readLine()).thenReturn("eof");
            final Logger logger = LogManager.getLogger("testGetMaxMapCountNumberFormatException");
            final MockLogAppender appender = new MockLogAppender();
            appender.start();
            appender.addExpectation(
                    new ParameterizedMessageLoggingExpectation(
                            "expected logged number format exception",
                            "testGetMaxMapCountNumberFormatException",
                            Level.WARN,
                            "unable to parse vm.max_map_count [{}]",
                            new Object[] { "eof" },
                            e -> e instanceof NumberFormatException && e.getMessage().equals("For input string: \"eof\"")));
            Loggers.addAppender(logger, appender);
            assertThat(check.getMaxMapCount(logger), equalTo(-1L));
            appender.assertAllExpectationsMatched();
            verify(reader).close();
            Loggers.removeAppender(logger, appender);
            appender.stop();
        }

    }

    private static class ParameterizedMessageLoggingExpectation implements MockLogAppender.LoggingExpectation {

        private boolean saw = false;

        private final String name;
        private final String loggerName;
        private final Level level;
        private final String messagePattern;
        private final Object[] arguments;
        private final Predicate<Throwable> throwablePredicate;

        private ParameterizedMessageLoggingExpectation(
                final String name,
                final String loggerName,
                final Level level,
                final String messagePattern,
                final Object[] arguments,
                final Predicate<Throwable> throwablePredicate) {
            this.name = name;
            this.loggerName = loggerName;
            this.level = level;
            this.messagePattern = messagePattern;
            this.arguments = arguments;
            this.throwablePredicate = throwablePredicate;
        }

        @Override
        public void match(final LogEvent event) {
            if (event.getLevel().equals(level) &&
                    event.getLoggerName().equals(loggerName) &&
                    event.getMessage() instanceof ParameterizedMessage) {
                final ParameterizedMessage message = (ParameterizedMessage)event.getMessage();
                saw = message.getFormat().equals(messagePattern) &&
                        Arrays.deepEquals(arguments, message.getParameters()) &&
                        throwablePredicate.test(event.getThrown());
            }
        }

        @Override
        public void assertMatched() {
            assertTrue(name, saw);
        }

    }

    public void testMaxMapCountCheckRead() throws IOException {
        final String rawProcSysVmMaxMapCount = Long.toString(randomIntBetween(1, Integer.MAX_VALUE));
        final BufferedReader reader = mock(BufferedReader.class);
        when(reader.readLine()).thenReturn(rawProcSysVmMaxMapCount);
        final BootstrapChecks.MaxMapCountCheck check = new BootstrapChecks.MaxMapCountCheck();
        assertThat(check.readProcSysVmMaxMapCount(reader), equalTo(rawProcSysVmMaxMapCount));
    }

    public void testMaxMapCountCheckParse() {
        final long procSysVmMaxMapCount = randomIntBetween(1, Integer.MAX_VALUE);
        final BootstrapChecks.MaxMapCountCheck check = new BootstrapChecks.MaxMapCountCheck();
        assertThat(check.parseProcSysVmMaxMapCount(Long.toString(procSysVmMaxMapCount)), equalTo(procSysVmMaxMapCount));
    }

}

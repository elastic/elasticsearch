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
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.util.Constants;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLogAppender;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.function.Predicate;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class MaxMapCountCheckTests extends ESTestCase {

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
            final Logger logger = ESLoggerFactory.getLogger("testGetMaxMapCountIOException");
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
            final Logger logger = ESLoggerFactory.getLogger("testGetMaxMapCountNumberFormatException");
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
        public void match(LogEvent event) {
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

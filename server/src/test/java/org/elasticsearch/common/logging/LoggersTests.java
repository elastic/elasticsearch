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

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.filter.RegexFilter;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Arrays;

import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class LoggersTests extends ESTestCase {

    static class MockAppender extends AbstractAppender {
        private LogEvent lastEvent;

        MockAppender(final String name) throws IllegalAccessException {
            super(name, RegexFilter.createFilter(".*(\n.*)*", new String[0], false, null, null), null);
        }

        @Override
        public void append(LogEvent event) {
            lastEvent = event.toImmutable();
        }

        ParameterizedMessage lastParameterizedMessage() {
            return (ParameterizedMessage) lastEvent.getMessage();
        }
    }

    public void testParameterizedMessageLambda() throws Exception {
        final MockAppender appender = new MockAppender("trace_appender");
        appender.start();
        final Logger testLogger = LogManager.getLogger(LoggersTests.class);
        Loggers.addAppender(testLogger, appender);
        Loggers.setLevel(testLogger, Level.TRACE);

        Throwable ex = randomException();
        testLogger.error(() -> new ParameterizedMessage("an error message"), ex);
        assertThat(appender.lastEvent.getLevel(), equalTo(Level.ERROR));
        assertThat(appender.lastEvent.getThrown(), equalTo(ex));
        assertThat(appender.lastParameterizedMessage().getFormattedMessage(), equalTo("an error message"));

        ex = randomException();
        testLogger.warn(() -> new ParameterizedMessage("a warn message: [{}]", "long gc"), ex);
        assertThat(appender.lastEvent.getLevel(), equalTo(Level.WARN));
        assertThat(appender.lastEvent.getThrown(), equalTo(ex));
        assertThat(appender.lastParameterizedMessage().getFormattedMessage(), equalTo("a warn message: [long gc]"));
        assertThat(appender.lastParameterizedMessage().getParameters(), arrayContaining("long gc"));

        testLogger.info(() -> new ParameterizedMessage("an info message a=[{}], b=[{}], c=[{}]", 1, 2, 3));
        assertThat(appender.lastEvent.getLevel(), equalTo(Level.INFO));
        assertThat(appender.lastEvent.getThrown(), nullValue());
        assertThat(appender.lastParameterizedMessage().getFormattedMessage(), equalTo("an info message a=[1], b=[2], c=[3]"));
        assertThat(appender.lastParameterizedMessage().getParameters(), arrayContaining(1, 2, 3));

        ex = randomException();
        testLogger.debug(() -> new ParameterizedMessage("a debug message options = {}", Arrays.asList("yes", "no")), ex);
        assertThat(appender.lastEvent.getLevel(), equalTo(Level.DEBUG));
        assertThat(appender.lastEvent.getThrown(), equalTo(ex));
        assertThat(appender.lastParameterizedMessage().getFormattedMessage(), equalTo("a debug message options = [yes, no]"));
        assertThat(appender.lastParameterizedMessage().getParameters(), arrayContaining(Arrays.asList("yes", "no")));

        ex = randomException();
        testLogger.trace(() -> new ParameterizedMessage("a trace message; element = [{}]", new Object[]{null}), ex);
        assertThat(appender.lastEvent.getLevel(), equalTo(Level.TRACE));
        assertThat(appender.lastEvent.getThrown(), equalTo(ex));
        assertThat(appender.lastParameterizedMessage().getFormattedMessage(), equalTo("a trace message; element = [null]"));
        assertThat(appender.lastParameterizedMessage().getParameters(), arrayContaining(new Object[]{null}));
    }

    private Throwable randomException(){
        return randomFrom(
            new IOException("file not found"),
            new UnknownHostException("unknown hostname"),
            new OutOfMemoryError("out of space"),
            new IllegalArgumentException("index must be between 10 and 100")
        );
    }
}

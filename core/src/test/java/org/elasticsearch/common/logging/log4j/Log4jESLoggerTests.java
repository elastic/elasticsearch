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

package org.elasticsearch.common.logging.log4j;

import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LocationInfo;
import org.apache.log4j.spi.LoggingEvent;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;
import org.junit.Test;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

public class Log4jESLoggerTests extends ESTestCase {

    private ESLogger esTestLogger;
    private TestAppender testAppender;
    private String testLevel;
    private DeprecationLogger deprecationLogger;
    private TestAppender deprecationAppender;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        this.testLevel = Log4jESLoggerFactory.getLogger("test").getLevel();
        LogConfigurator.reset();
        Path configDir = getDataPath("config");
        // Need to set custom path.conf so we can use a custom logging.yml file for the test
        Settings settings = Settings.builder()
                .put("path.conf", configDir.toAbsolutePath())
                .put("path.home", createTempDir().toString())
                .build();
        LogConfigurator.configure(settings);

        esTestLogger = Log4jESLoggerFactory.getLogger("test");
        Logger testLogger = ((Log4jESLogger) esTestLogger).logger();
        assertThat(testLogger.getLevel(), equalTo(Level.TRACE));
        testAppender = new TestAppender();
        testLogger.addAppender(testAppender);

        // deprecation setup, needs to be set to debug to log
        deprecationLogger = Log4jESLoggerFactory.getDeprecationLogger("test");
        deprecationAppender = new TestAppender();
        ESLogger logger = Log4jESLoggerFactory.getLogger("deprecation.test");
        logger.setLevel("DEBUG");
        (((Log4jESLogger) logger).logger()).addAppender(deprecationAppender);
    }

    @Override
    @After
    public void tearDown() throws Exception {
        super.tearDown();
        esTestLogger.setLevel(testLevel);
        Logger testLogger = ((Log4jESLogger) esTestLogger).logger();
        testLogger.removeAppender(testAppender);
        Logger deprecationLogger = ((Log4jESLogger) Log4jESLoggerFactory.getLogger("deprecation.test")).logger();
        deprecationLogger.removeAppender(deprecationAppender);
    }

    @Test
    public void locationInfoTest() {
        esTestLogger.error("This is an error");
        esTestLogger.warn("This is a warning");
        esTestLogger.info("This is an info");
        esTestLogger.debug("This is a debug");
        esTestLogger.trace("This is a trace");
        List<LoggingEvent> events = testAppender.getEvents();
        assertThat(events, notNullValue());
        assertThat(events.size(), equalTo(5));
        LoggingEvent event = events.get(0);
        assertThat(event, notNullValue());
        assertThat(event.getLevel(), equalTo(Level.ERROR));
        assertThat(event.getRenderedMessage(), equalTo("This is an error"));
        LocationInfo locationInfo = event.getLocationInformation();
        assertThat(locationInfo, notNullValue());
        assertThat(locationInfo.getClassName(), equalTo(Log4jESLoggerTests.class.getCanonicalName()));
        assertThat(locationInfo.getMethodName(), equalTo("locationInfoTest"));
        event = events.get(1);
        assertThat(event, notNullValue());
        assertThat(event.getLevel(), equalTo(Level.WARN));
        assertThat(event.getRenderedMessage(), equalTo("This is a warning"));
        locationInfo = event.getLocationInformation();
        assertThat(locationInfo, notNullValue());
        assertThat(locationInfo.getClassName(), equalTo(Log4jESLoggerTests.class.getCanonicalName()));
        assertThat(locationInfo.getMethodName(), equalTo("locationInfoTest"));
        event = events.get(2);
        assertThat(event, notNullValue());
        assertThat(event.getLevel(), equalTo(Level.INFO));
        assertThat(event.getRenderedMessage(), equalTo("This is an info"));
        locationInfo = event.getLocationInformation();
        assertThat(locationInfo, notNullValue());
        assertThat(locationInfo.getClassName(), equalTo(Log4jESLoggerTests.class.getCanonicalName()));
        assertThat(locationInfo.getMethodName(), equalTo("locationInfoTest"));
        event = events.get(3);
        assertThat(event, notNullValue());
        assertThat(event.getLevel(), equalTo(Level.DEBUG));
        assertThat(event.getRenderedMessage(), equalTo("This is a debug"));
        locationInfo = event.getLocationInformation();
        assertThat(locationInfo, notNullValue());
        assertThat(locationInfo.getClassName(), equalTo(Log4jESLoggerTests.class.getCanonicalName()));
        assertThat(locationInfo.getMethodName(), equalTo("locationInfoTest"));
        event = events.get(4);
        assertThat(event, notNullValue());
        assertThat(event.getLevel(), equalTo(Level.TRACE));
        assertThat(event.getRenderedMessage(), equalTo("This is a trace"));
        locationInfo = event.getLocationInformation();
        assertThat(locationInfo, notNullValue());
        assertThat(locationInfo.getClassName(), equalTo(Log4jESLoggerTests.class.getCanonicalName()));
        assertThat(locationInfo.getMethodName(), equalTo("locationInfoTest"));
    }

    @Test
    public void testDeprecationLogger() {
        deprecationLogger.deprecated("This is a deprecation message");
        List<LoggingEvent> deprecationEvents = deprecationAppender.getEvents();
        LoggingEvent event = deprecationEvents.get(0);
        assertThat(event, notNullValue());
        assertThat(event.getLevel(), equalTo(Level.DEBUG));
        assertThat(event.getRenderedMessage(), equalTo("This is a deprecation message"));
    }

    private static class TestAppender extends AppenderSkeleton {

        private List<LoggingEvent> events = new ArrayList<>();

        @Override
        public void close() {
        }

        @Override
        public boolean requiresLayout() {
            return false;
        }

        @Override
        protected void append(LoggingEvent event) {
            // Forces it to generate the location information
            event.getLocationInformation();
            events.add(event);
        }

        public List<LoggingEvent> getEvents() {
            return events;
        }
    }
}

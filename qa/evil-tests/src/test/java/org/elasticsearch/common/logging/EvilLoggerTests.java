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
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configurator;
import org.elasticsearch.Version;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.hamcrest.RegexMatcher;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.hamcrest.Matchers.equalTo;

public class EvilLoggerTests extends ESTestCase {

    private Logger testLogger;
    private DeprecationLogger deprecationLogger;

    @Override
    public void setUp() throws Exception {
        super.setUp();

        final Path configDir = getDataPath("config");
        // need to set custom path.conf so we can use a custom log4j2.properties file for the test
        final Settings settings = Settings.builder()
            .put(Environment.PATH_CONF_SETTING.getKey(), configDir.toAbsolutePath())
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
            .build();
        final Environment environment = new Environment(settings);
        LogConfigurator.configure(environment, true);

        testLogger = ESLoggerFactory.getLogger("test");
        deprecationLogger = ESLoggerFactory.getDeprecationLogger("test");
    }

    @Override
    public void tearDown() throws Exception {
        LoggerContext context = (LoggerContext) LogManager.getContext(false);
        Configurator.shutdown(context);
        super.tearDown();
    }

    public void testLocationInfoTest() throws IOException {
        testLogger.error("This is an error message");
        testLogger.warn("This is a warning message");
        testLogger.info("This is an info message");
        testLogger.debug("This is a debug message");
        testLogger.trace("This is a trace message");
        final String path = System.getProperty("es.logs") + ".log";
        final List<String> events = Files.readAllLines(PathUtils.get(path));
        assertThat(events.size(), equalTo(5));
        final String location = "org.elasticsearch.common.logging.EvilLoggerTests.testLocationInfoTest";
        // the first message is a warning for unsupported configuration files
        assertLogLine(events.get(0), Level.ERROR, location, "This is an error message");
        assertLogLine(events.get(1), Level.WARN, location, "This is a warning message");
        assertLogLine(events.get(2), Level.INFO, location, "This is an info message");
        assertLogLine(events.get(3), Level.DEBUG, location, "This is a debug message");
        assertLogLine(events.get(4), Level.TRACE, location, "This is a trace message");
    }

    private void assertLogLine(final String logLine, final Level level, final String location, final String message) {
        final Matcher matcher = Pattern.compile("\\[(.*)\\]\\[(.*)\\(.*\\)\\] (.*)").matcher(logLine);
        assertTrue(logLine, matcher.matches());
        assertThat(matcher.group(1), equalTo(level.toString()));
        assertThat(matcher.group(2), RegexMatcher.matches(location));
        assertThat(matcher.group(3), RegexMatcher.matches(message));
    }

    public void testDeprecationLogger() throws IOException {
        deprecationLogger.deprecated("This is a deprecation message");
        final String deprecationPath = System.getProperty("es.logs") + "_deprecation.log";
        final List<String> deprecationEvents = Files.readAllLines(PathUtils.get(deprecationPath));
        assertThat(deprecationEvents.size(), equalTo(1));
        assertLogLine(
            deprecationEvents.get(0),
            Level.WARN,
            "org.elasticsearch.common.logging.DeprecationLogger.deprecated",
            "This is a deprecation message");
    }

}

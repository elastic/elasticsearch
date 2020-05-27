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
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.ConsoleAppender;
import org.apache.logging.log4j.core.appender.CountingNoOpAppender;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.util.Constants;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.env.Environment;
import org.elasticsearch.node.Node;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.hamcrest.RegexMatcher;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.startsWith;

public class EvilLoggerTests extends ESTestCase {

    @Override
    public void setUp() throws Exception {
        super.setUp();
        LogConfigurator.registerErrorListener();
    }

    @Override
    public void tearDown() throws Exception {
        LoggerContext context = (LoggerContext) LogManager.getContext(false);
        Configurator.shutdown(context);
        super.tearDown();
    }

    public void testLocationInfoTest() throws IOException, UserException {
        setupLogging("location_info");

        final Logger testLogger = LogManager.getLogger("test");

        testLogger.error("This is an error message");
        testLogger.warn("This is a warning message");
        testLogger.info("This is an info message");
        testLogger.debug("This is a debug message");
        testLogger.trace("This is a trace message");
        final String path =
            System.getProperty("es.logs.base_path") +
                System.getProperty("file.separator") +
                System.getProperty("es.logs.cluster_name") +
                ".log";
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

    public void testConcurrentDeprecationLogger() throws IOException, UserException, BrokenBarrierException, InterruptedException {
        setupLogging("deprecation");

        final DeprecationLogger deprecationLogger = new DeprecationLogger(LogManager.getLogger("deprecation"));

        final int numberOfThreads = randomIntBetween(2, 4);
        final CyclicBarrier barrier = new CyclicBarrier(1 + numberOfThreads);
        final List<Thread> threads = new ArrayList<>();
        final int iterations = randomIntBetween(1, 4);
        for (int i = 0; i < numberOfThreads; i++) {
            final Thread thread = new Thread(() -> {
                final List<Integer> ids = IntStream.range(0, 128).boxed().collect(Collectors.toList());
                Randomness.shuffle(ids);
                final ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
                DeprecationLogger.setThreadContext(threadContext);
                try {
                    barrier.await();
                } catch (final BrokenBarrierException | InterruptedException e) {
                    throw new RuntimeException(e);
                }
                for (int j = 0; j < iterations; j++) {
                    for (final Integer id : ids) {
                        deprecationLogger.deprecatedAndMaybeLog(Integer.toString(id), "This is a maybe logged deprecation message" + id);
                    }
                }

                /*
                 * We have to manually check that each thread has the right warning headers in the thread context because the act of doing
                 * this through the test framework on one thread would otherwise clear the thread context and we would be unable to assert
                 * on the other threads.
                 */
                final List<String> warnings = threadContext.getResponseHeaders().get("Warning");
                final Set<String> actualWarningValues =
                        warnings.stream().map(s -> DeprecationLogger.extractWarningValueFromWarningHeader(s, true))
                            .collect(Collectors.toSet());
                for (int j = 0; j < 128; j++) {
                    assertThat(
                            actualWarningValues,
                            hasItem(DeprecationLogger.escapeAndEncode("This is a maybe logged deprecation message" + j)));
                }

                try {
                    barrier.await();
                } catch (final BrokenBarrierException | InterruptedException e) {
                    throw new RuntimeException(e);
                }
            });
            threads.add(thread);
            thread.start();
        }

        // synchronize the start of all threads
        barrier.await();

        // wait for all threads to complete their iterations
        barrier.await();

        final String deprecationPath =
                System.getProperty("es.logs.base_path") +
                        System.getProperty("file.separator") +
                        System.getProperty("es.logs.cluster_name") +
                        "_deprecation.log";
        final List<String> deprecationEvents = Files.readAllLines(PathUtils.get(deprecationPath));
        // we appended an integer to each log message, use that for sorting
        Pattern pattern = Pattern.compile(".*message(\\d+)\"");
        deprecationEvents.sort(Comparator.comparingInt(s -> {
            Matcher matcher = pattern.matcher(s);
            matcher.matches();
            return Integer.parseInt(matcher.group(1));
        }));
        assertThat(deprecationEvents.size(), equalTo(128));

        for (int i = 0; i < 128; i++) {
            assertLogLine(
                    deprecationEvents.get(i),
                    Level.WARN,
                    "org.elasticsearch.common.logging.DeprecationLogger\\$2\\.run",
                    "This is a maybe logged deprecation message" + i);
        }

        for (final Thread thread : threads) {
            thread.join();
        }

    }

    public void testDeprecationLoggerMaybeLog() throws IOException, UserException {
        setupLogging("deprecation");

        final DeprecationLogger deprecationLogger = new DeprecationLogger(LogManager.getLogger("deprecation"));

        final int iterations = randomIntBetween(1, 16);

        for (int i = 0; i < iterations; i++) {
            deprecationLogger.deprecatedAndMaybeLog("key", "This is a maybe logged deprecation message");
            assertWarnings("This is a maybe logged deprecation message");
        }
        for (int k = 0; k < 128; k++) {
            for (int i = 0; i < iterations; i++) {
                deprecationLogger.deprecatedAndMaybeLog("key" + k, "This is a maybe logged deprecation message" + k);
                assertWarnings("This is a maybe logged deprecation message" + k);
            }
        }
        for (int i = 0; i < iterations; i++) {
            deprecationLogger.deprecatedAndMaybeLog("key", "This is a maybe logged deprecation message");
            assertWarnings("This is a maybe logged deprecation message");
        }

        final String deprecationPath =
                System.getProperty("es.logs.base_path") +
                        System.getProperty("file.separator") +
                        System.getProperty("es.logs.cluster_name") +
                        "_deprecation.log";
        final List<String> deprecationEvents = Files.readAllLines(PathUtils.get(deprecationPath));
        assertThat(deprecationEvents.size(), equalTo(1 + 128 + 1));
        assertLogLine(
                deprecationEvents.get(0),
                Level.WARN,
                "org.elasticsearch.common.logging.DeprecationLogger\\$2\\.run",
                "This is a maybe logged deprecation message");
        for (int k = 0; k < 128; k++) {
            assertLogLine(
                    deprecationEvents.get(1 + k),
                    Level.WARN,
                    "org.elasticsearch.common.logging.DeprecationLogger\\$2\\.run",
                    "This is a maybe logged deprecation message" + k);
        }
    }

    public void testDeprecatedSettings() throws IOException, UserException {
        setupLogging("settings");

        final Setting<Boolean> setting = Setting.boolSetting("deprecated.foo", false, Setting.Property.Deprecated);
        final Settings settings = Settings.builder().put("deprecated.foo", true).build();

        final int iterations = randomIntBetween(0, 128);
        for (int i = 0; i < iterations; i++) {
            setting.get(settings);
            assertSettingDeprecationsAndWarnings(new Setting<?>[]{setting});
        }

        final String deprecationPath =
                System.getProperty("es.logs.base_path") +
                        System.getProperty("file.separator") +
                        System.getProperty("es.logs.cluster_name") +
                        "_deprecation.log";
        final List<String> deprecationEvents = Files.readAllLines(PathUtils.get(deprecationPath));
        if (iterations > 0) {
            assertThat(deprecationEvents.size(), equalTo(1));
            assertLogLine(
                    deprecationEvents.get(0),
                    Level.WARN,
                    "org.elasticsearch.common.logging.DeprecationLogger\\$2\\.run",
                    "\\[deprecated.foo\\] setting was deprecated in Elasticsearch and will be removed in a future release! " +
                            "See the breaking changes documentation for the next major version.");
        }
    }

    public void testFindAppender() throws IOException, UserException {
        setupLogging("find_appender");

        final Logger hasConsoleAppender = LogManager.getLogger("has_console_appender");

        final Appender testLoggerConsoleAppender = Loggers.findAppender(hasConsoleAppender, ConsoleAppender.class);
        assertNotNull(testLoggerConsoleAppender);
        assertThat(testLoggerConsoleAppender.getName(), equalTo("console"));
        final Logger hasCountingNoOpAppender = LogManager.getLogger("has_counting_no_op_appender");
        assertNull(Loggers.findAppender(hasCountingNoOpAppender, ConsoleAppender.class));
        final Appender countingNoOpAppender = Loggers.findAppender(hasCountingNoOpAppender, CountingNoOpAppender.class);
        assertThat(countingNoOpAppender.getName(), equalTo("counting_no_op"));
    }

    public void testPrefixLogger() throws IOException, IllegalAccessException, UserException {
        setupLogging("prefix");

        final String prefix = randomAlphaOfLength(16);
        final Logger logger = new PrefixLogger(LogManager.getLogger("prefix_test"), prefix);
        logger.info("test");
        logger.info("{}", "test");
        final Exception e = new Exception("exception");
        logger.info(new ParameterizedMessage("{}", "test"), e);

        final String path =
            System.getProperty("es.logs.base_path") +
                System.getProperty("file.separator") +
                System.getProperty("es.logs.cluster_name") +
                ".log";
        final List<String> events = Files.readAllLines(PathUtils.get(path));

        final StringWriter sw = new StringWriter();
        final PrintWriter pw = new PrintWriter(sw);
        e.printStackTrace(pw);
        final int stackTraceLength = sw.toString().split(System.getProperty("line.separator")).length;
        final int expectedLogLines = 3;
        assertThat(events.size(), equalTo(expectedLogLines + stackTraceLength));
        for (int i = 0; i < expectedLogLines; i++) {
            assertThat("Contents of [" + path + "] are wrong",
                    events.get(i), startsWith("[" + getTestName() + "]" + prefix + " test"));
        }
    }

    public void testPrefixLoggerMarkersCanBeCollected() throws IOException, UserException {
        setupLogging("prefix");

        final int prefixes = 1 << 19; // to ensure enough markers that the GC should collect some when we force a GC below
        for (int i = 0; i < prefixes; i++) {
            // this has the side effect of caching a marker with this prefix
            new PrefixLogger(LogManager.getLogger("logger" + i), "prefix" + i);
        }

        System.gc(); // this will free the weakly referenced keys in the marker cache
        assertThat(PrefixLogger.markersSize(), lessThan(prefixes));
    }

    public void testProperties() throws IOException, UserException {
        final Settings settings = Settings.builder()
                .put("cluster.name", randomAlphaOfLength(16))
                .put("node.name", randomAlphaOfLength(16))
                .build();
        setupLogging("minimal", settings);

        assertNotNull(System.getProperty("es.logs.base_path"));

        assertThat(System.getProperty("es.logs.cluster_name"), equalTo(ClusterName.CLUSTER_NAME_SETTING.get(settings).value()));
        assertThat(System.getProperty("es.logs.node_name"), equalTo(Node.NODE_NAME_SETTING.get(settings)));
    }

    public void testNoNodeNameInPatternWarning() throws IOException, UserException {
        String nodeName = randomAlphaOfLength(16);
        LogConfigurator.setNodeName(nodeName);
        setupLogging("no_node_name");
        final String path =
            System.getProperty("es.logs.base_path") +
                System.getProperty("file.separator") +
                System.getProperty("es.logs.cluster_name") + ".log";
        final List<String> events = Files.readAllLines(PathUtils.get(path));
        assertThat(events.size(), equalTo(2));
        final String location = "org.elasticsearch.common.logging.LogConfigurator";
        // the first message is a warning for unsupported configuration files
        assertLogLine(events.get(0), Level.WARN, location, "\\[" + nodeName + "\\] Some logging configurations have "
                + "%marker but don't have %node_name. We will automatically add %node_name to the pattern to ease the "
                + "migration for users who customize log4j2.properties but will stop this behavior in 7.0. You should "
                + "manually replace `%node_name` with `\\[%node_name\\]%marker ` in these locations:");
        if (Constants.WINDOWS) {
            assertThat(events.get(1), endsWith("no_node_name\\log4j2.properties"));
        } else {
            assertThat(events.get(1), endsWith("no_node_name/log4j2.properties"));
        }
    }

    private void setupLogging(final String config) throws IOException, UserException {
        setupLogging(config, Settings.EMPTY);
    }

    private void setupLogging(final String config, final Settings settings) throws IOException, UserException {
        assert !Environment.PATH_HOME_SETTING.exists(settings);
        final Path configDir = getDataPath(config);
        final Settings mergedSettings = Settings.builder()
            .put(settings)
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
            .build();
        // need to use custom config path so we can use a custom log4j2.properties file for the test
        final Environment environment = new Environment(mergedSettings, configDir);
        LogConfigurator.configure(environment);
    }

    private void assertLogLine(final String logLine, final Level level, final String location, final String message) {
        final Matcher matcher = Pattern.compile("\\[(.*)\\]\\[(.*)\\(.*\\)\\] (.*)").matcher(logLine);
        assertTrue(logLine, matcher.matches());
        assertThat(matcher.group(1), equalTo(level.toString()));
        assertThat(matcher.group(2), RegexMatcher.matches(location));
        assertThat(matcher.group(3), RegexMatcher.matches(message));
    }

}

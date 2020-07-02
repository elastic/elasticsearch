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
import org.elasticsearch.cli.UserException;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.FeatureMatcher;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.junit.BeforeClass;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.matchesRegex;
import static org.hamcrest.Matchers.not;

/**
 * This test confirms JSON log structure is properly formatted and can be parsed.
 * It has to be in a <code>org.elasticsearch.common.logging</code> package to use <code>PrefixLogger</code>.
 * When running from IDE set -Dtests.security.manager=false
 */
public class JsonLoggerTests extends ESTestCase {

    private static final String LINE_SEPARATOR = System.lineSeparator();

    @BeforeClass
    public static void initNodeName() {
        assert "false".equals(System.getProperty("tests.security.manager")) : "-Dtests.security.manager=false has to be set";
        JsonLogsTestSetup.init();
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        LogConfigurator.registerErrorListener();
        setupLogging("json_layout");
    }

    @Override
    public void tearDown() throws Exception {
        LoggerContext context = (LoggerContext) LogManager.getContext(false);
        Configurator.shutdown(context);
        super.tearDown();
    }

    public void testDeprecatedMessage() throws IOException {
        final Logger testLogger = LogManager.getLogger("deprecation.test");
        testLogger.info(DeprecatedMessage.of("someId","deprecated message1"));

        final Path path = PathUtils.get(System.getProperty("es.logs.base_path"),
            System.getProperty("es.logs.cluster_name") + "_deprecated.json");
        try (Stream<Map<String, String>> stream = JsonLogsStream.mapStreamFrom(path)) {
            List<Map<String, String>> jsonLogs = stream
                .collect(Collectors.toList());

            assertThat(jsonLogs, contains(
                allOf(
                    hasEntry("type", "deprecation"),
                    hasEntry("log.level", "INFO"),
                    hasEntry("log.logger", "deprecation.test"),
                    hasEntry("cluster.name", "elasticsearch"),
                    hasEntry("node.name", "sample-name"),
                    hasEntry("message", "deprecated message1"),
                    hasEntry("x-opaque-id", "someId"))
                )
            );
        }
    }

    public void testBuildingMessage() throws IOException {

        final Logger testLogger = LogManager.getLogger("test");

        testLogger.info(new ESLogMessage("some message {} {}", "value0")
                                    .argAndField("key1","value1")
                                    .field("key2","value2"));

        final Path path = PathUtils.get(System.getProperty("es.logs.base_path"),
            System.getProperty("es.logs.cluster_name") + ".json");
        try (Stream<Map<String, String>> stream = JsonLogsStream.mapStreamFrom(path)) {
            List<Map<String, String>> jsonLogs = stream
                .collect(Collectors.toList());

            assertThat(jsonLogs, contains(
                allOf(
                    hasEntry("type", "file"),
                    hasEntry("log.level", "INFO"),
                    hasEntry("log.logger", "test"),
                    hasEntry("cluster.name", "elasticsearch"),
                    hasEntry("node.name", "sample-name"),
                    hasEntry("message", "some message value0 value1"),
                    hasEntry("key1", "value1"),
                    hasEntry("key2", "value2"))
                )
            );
        }
    }

    public void testCustomMessageWithMultipleFields() throws IOException {
        // if a field is defined to be overriden, it has to always be overriden in that appender.
        final Logger testLogger = LogManager.getLogger("test");
        testLogger.info(new ESLogMessage("some message")
                                    .with("field1","value1")
                                    .with("field2","value2"));

        final Path path = PathUtils.get(System.getProperty("es.logs.base_path"),
            System.getProperty("es.logs.cluster_name") + ".json");
        try (Stream<Map<String, String>> stream = JsonLogsStream.mapStreamFrom(path)) {
            List<Map<String, String>> jsonLogs = stream
                .collect(Collectors.toList());

            assertThat(jsonLogs, contains(
                allOf(
                    hasEntry("type", "file"),
                    hasEntry("log.level", "INFO"),
                    hasEntry("log.logger", "test"),
                    hasEntry("cluster.name", "elasticsearch"),
                    hasEntry("node.name", "sample-name"),
                    hasEntry("field1", "value1"),
                    hasEntry("field2", "value2"),
                    hasEntry("message", "some message"))
                )
            );
        }
    }


    public void testDeprecatedMessageWithoutXOpaqueId() throws IOException {
        final Logger testLogger = LogManager.getLogger("deprecation.test");
        testLogger.info( DeprecatedMessage.of("someId","deprecated message1"));
        testLogger.info( DeprecatedMessage.of("","deprecated message2"));
        testLogger.info( DeprecatedMessage.of(null,"deprecated message3"));
        testLogger.info("deprecated message4");

        final Path path = PathUtils.get(System.getProperty("es.logs.base_path"),
            System.getProperty("es.logs.cluster_name") + "_deprecated.json");
        try (Stream<Map<String, String>> stream = JsonLogsStream.mapStreamFrom(path)) {
            List<Map<String, String>> jsonLogs = stream
                .collect(Collectors.toList());

            assertThat(jsonLogs, contains(
                allOf(
                    hasEntry("type", "deprecation"),
                    hasEntry("log.level", "INFO"),
                    hasEntry("log.logger", "deprecation.test"),
                    hasEntry("cluster.name", "elasticsearch"),
                    hasEntry("node.name", "sample-name"),
                    hasEntry("message", "deprecated message1"),
                    hasEntry("x-opaque-id", "someId")),
                allOf(
                    hasEntry("type", "deprecation"),
                    hasEntry("log.level", "INFO"),
                    hasEntry("log.logger", "deprecation.test"),
                    hasEntry("cluster.name", "elasticsearch"),
                    hasEntry("node.name", "sample-name"),
                    hasEntry("message", "deprecated message2"),
                    not(hasKey("x-opaque-id"))
                ),
                allOf(
                    hasEntry("type", "deprecation"),
                    hasEntry("log.level", "INFO"),
                    hasEntry("log.logger", "deprecation.test"),
                    hasEntry("cluster.name", "elasticsearch"),
                    hasEntry("node.name", "sample-name"),
                    hasEntry("message", "deprecated message3"),
                    not(hasKey("x-opaque-id"))
                ),
                allOf(
                    hasEntry("type", "deprecation"),
                    hasEntry("log.level", "INFO"),
                    hasEntry("log.logger", "deprecation.test"),
                    hasEntry("cluster.name", "elasticsearch"),
                    hasEntry("node.name", "sample-name"),
                    hasEntry("message", "deprecated message4"),
                    not(hasKey("x-opaque-id"))
                )
                )
            );
        }
    }

    public void testJsonLayout() throws IOException {
        final Logger testLogger = LogManager.getLogger("test");

        testLogger.error("This is an error message");
        testLogger.warn("This is a warning message");
        testLogger.info("This is an info message");
        testLogger.debug("This is a debug message");
        testLogger.trace("This is a trace message");
        final Path path = clusterLogsPath();
        try (Stream<JsonLogLine> stream = JsonLogsStream.from(path)) {
            List<JsonLogLine> jsonLogs = collectLines(stream);

            assertThat(jsonLogs, contains(
                logLine("file", Level.ERROR, "sample-name", "test", "This is an error message"),
                logLine("file", Level.WARN, "sample-name", "test", "This is a warning message"),
                logLine("file", Level.INFO, "sample-name", "test", "This is an info message"),
                logLine("file", Level.DEBUG, "sample-name", "test", "This is a debug message"),
                logLine("file", Level.TRACE, "sample-name", "test", "This is a trace message")
            ));
        }
    }

    public void testPrefixLoggerInJson() throws IOException {
        Logger shardIdLogger = Loggers.getLogger("prefix.shardIdLogger", ShardId.fromString("[indexName][123]"));
        shardIdLogger.info("This is an info message with a shardId");

        Logger prefixLogger = new PrefixLogger(LogManager.getLogger("prefix.prefixLogger"), "PREFIX");
        prefixLogger.info("This is an info message with a prefix");

        final Path path = clusterLogsPath();
        try (Stream<JsonLogLine> stream = JsonLogsStream.from(path)) {
            List<JsonLogLine> jsonLogs = collectLines(stream);
            assertThat(jsonLogs, contains(
                logLine("file", Level.INFO, "sample-name", "prefix.shardIdLogger",
                    "This is an info message with a shardId", Map.of(JsonLogLine::getTags, List.of("[indexName][123]"))),
                logLine("file", Level.INFO, "sample-name", "prefix.prefixLogger",
                    "This is an info message with a prefix", Map.of(JsonLogLine::getTags, List.of("PREFIX")))
            ));
        }
    }

    public void testJsonInMessage() throws IOException {
        final Logger testLogger = LogManager.getLogger("test");
        String json = "{" + LINE_SEPARATOR +
            "  \"terms\" : {" + LINE_SEPARATOR +
            "    \"user\" : [" + LINE_SEPARATOR +
            "      \"u1\"," + LINE_SEPARATOR +
            "      \"u2\"," + LINE_SEPARATOR +
            "      \"u3\"" + LINE_SEPARATOR +
            "    ]," + LINE_SEPARATOR +
            "    \"boost\" : 1.0" + LINE_SEPARATOR +
            "  }" + LINE_SEPARATOR +
            "}";

        testLogger.info(json);

        final Path path = clusterLogsPath();
        try (Stream<JsonLogLine> stream = JsonLogsStream.from(path)) {
            List<JsonLogLine> jsonLogs = collectLines(stream);
            assertThat(jsonLogs, contains(
                logLine("file", Level.INFO, "sample-name", "test", json)
            ));
        }
    }

    public void testStacktrace() throws IOException {
        final Logger testLogger = LogManager.getLogger("test");
        testLogger.error("error message", new Exception("exception message", new RuntimeException("cause message")));

        final Path path = clusterLogsPath();
        try (Stream<JsonLogLine> stream = JsonLogsStream.from(path)) {
            List<JsonLogLine> jsonLogs = collectLines(stream);
            assertThat(jsonLogs, contains(
                allOf(
                    logLine("file", Level.ERROR, "sample-name", "test", "error message"),
                    stacktraceMatches("java.lang.Exception: exception message.*Caused by: java.lang.RuntimeException: cause message.*")
                )
            ));
        }
    }


    public void testJsonInStacktraceMessageIsNotSplitted() throws IOException {
        final Logger testLogger = LogManager.getLogger("test");

        String json = "{" + LINE_SEPARATOR +
            "  \"terms\" : {" + LINE_SEPARATOR +
            "    \"user\" : [" + LINE_SEPARATOR +
            "      \"u1\"," + LINE_SEPARATOR +
            "      \"u2\"," + LINE_SEPARATOR +
            "      \"u3\"" + LINE_SEPARATOR +
            "    ]," + LINE_SEPARATOR +
            "    \"boost\" : 1.0" + LINE_SEPARATOR +
            "  }" + LINE_SEPARATOR +
            "}";
        testLogger.error("error message " + json, new Exception(json));

        final Path path = clusterLogsPath();
        try (Stream<JsonLogLine> stream = JsonLogsStream.from(path)) {
            List<JsonLogLine> jsonLogs = collectLines(stream);

            assertThat(jsonLogs, contains(
                allOf(
                    //message field will have a single line with json escaped
                    logLine("file", Level.ERROR, "sample-name", "test", "error message " + json),

                    //stacktrace message will be single line
                    stacktraceWith("java.lang.Exception: " + json)
                )
            ));
        }
    }


    public void testDuplicateLogMessages() throws IOException {
        final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger("test");

        // For the same key and X-Opaque-ID deprecation should be once
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
            threadContext.putHeader(Task.X_OPAQUE_ID, "ID1");
            DeprecationLogger.setThreadContext(threadContext);
            deprecationLogger.deprecate("key", "message1");
            deprecationLogger.deprecate("key", "message2");
            assertWarnings("message1", "message2");

            final Path path = PathUtils.get(System.getProperty("es.logs.base_path"),
                System.getProperty("es.logs.cluster_name") + "_deprecated.json");
            try (Stream<Map<String, String>> stream = JsonLogsStream.mapStreamFrom(path)) {
                List<Map<String, String>> jsonLogs = stream
                    .collect(Collectors.toList());

                assertThat(jsonLogs, contains(
                    allOf(
                        hasEntry("type", "deprecation"),
                        hasEntry("log.level", "WARN"),
                        hasEntry("log.logger", "deprecation.test"),
                        hasEntry("cluster.name", "elasticsearch"),
                        hasEntry("node.name", "sample-name"),
                        hasEntry("message", "message1"),
                        hasEntry("x-opaque-id", "ID1"))
                    )
                );
            }
        } finally {
            DeprecationLogger.removeThreadContext(threadContext);
        }


        // For the same key and different X-Opaque-ID should be multiple times per key/x-opaque-id
        //continuing with message1-ID1 in logs already, adding a new deprecation log line with message2-ID2
        try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
            threadContext.putHeader(Task.X_OPAQUE_ID, "ID2");
            DeprecationLogger.setThreadContext(threadContext);
            deprecationLogger.deprecate("key", "message1");
            deprecationLogger.deprecate("key", "message2");
            assertWarnings("message1", "message2");

            final Path path = PathUtils.get(System.getProperty("es.logs.base_path"),
                System.getProperty("es.logs.cluster_name") + "_deprecated.json");
            try (Stream<Map<String, String>> stream = JsonLogsStream.mapStreamFrom(path)) {
                List<Map<String, String>> jsonLogs = stream
                    .collect(Collectors.toList());

                assertThat(jsonLogs, contains(
                    allOf(
                        hasEntry("type", "deprecation"),
                        hasEntry("log.level", "WARN"),
                        hasEntry("log.logger", "deprecation.test"),
                        hasEntry("cluster.name", "elasticsearch"),
                        hasEntry("node.name", "sample-name"),
                        hasEntry("message", "message1"),
                        hasEntry("x-opaque-id", "ID1")
                    ),
                    allOf(
                        hasEntry("type", "deprecation"),
                        hasEntry("log.level", "WARN"),
                        hasEntry("log.logger", "deprecation.test"),
                        hasEntry("cluster.name", "elasticsearch"),
                        hasEntry("node.name", "sample-name"),
                        hasEntry("message", "message1"),
                        hasEntry("x-opaque-id", "ID2")
                    )
                    )
                );
            }
        } finally {
            DeprecationLogger.removeThreadContext(threadContext);
        }
    }

    private List<JsonLogLine> collectLines(Stream<JsonLogLine> stream) {
        return stream
            .collect(Collectors.toList());
    }

    private Path clusterLogsPath() {
        return PathUtils.get(System.getProperty("es.logs.base_path"), System.getProperty("es.logs.cluster_name") + ".json");
    }

    private void setupLogging(final String config) throws IOException, UserException {
        setupLogging(config, Settings.EMPTY);
    }

    private void setupLogging(final String config, final Settings settings) throws IOException, UserException {
        assertFalse("Environment path.home variable should not be set", Environment.PATH_HOME_SETTING.exists(settings));
        final Path configDir = getDataPath(config);
        final Settings mergedSettings = Settings.builder()
                                                .put(settings)
                                                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
                                                .build();
        // need to use custom config path so we can use a custom log4j2.properties file for the test
        final Environment environment = new Environment(mergedSettings, configDir);
        LogConfigurator.configure(environment);
    }


    private Matcher<JsonLogLine> logLine(String type, Level level, String nodeName, String component, String message) {
        return logLine(mapOfParamsToCheck(type, level, nodeName, component, message));
    }

    private Map<Function<JsonLogLine, Object>, Object> mapOfParamsToCheck(
        String type, Level level, String nodeName, String component, String message) {
        return Map.of(JsonLogLine::getType, type,
            JsonLogLine::getLevel, level.toString(),
            JsonLogLine::getNodeName, nodeName,
            JsonLogLine::getComponent, component,
            JsonLogLine::getMessage, message);
    }

    private Matcher<JsonLogLine> logLine(String type, Level level, String nodeName, String component, String message,
                                         Map<Function<JsonLogLine,Object>, Object> additionalProperties) {
        Map<Function<JsonLogLine, Object>, Object> map = new HashMap<>();
        map.putAll(mapOfParamsToCheck(type, level, nodeName, component, message));
        map.putAll(additionalProperties);
        return logLine(map);
    }

    private Matcher<JsonLogLine> logLine(Map<Function<JsonLogLine,Object>, Object> map) {
        return new FeatureMatcher<JsonLogLine, Boolean>(Matchers.is(true), "logLine", "logLine") {

            @Override
            protected Boolean featureValueOf(JsonLogLine actual) {
                return map.entrySet()
                    .stream()
                    .allMatch(entry -> Objects.equals(entry.getKey().apply(actual), entry.getValue()));
            }
        };
    }
    private Matcher<JsonLogLine> stacktraceWith(String line) {
        return new FeatureMatcher<JsonLogLine, List<String>>(hasItems(Matchers.containsString(line)),
            "error.stack_trace", "error.stack_trace") {

            @Override
            protected List<String> featureValueOf(JsonLogLine actual) {
                return actual.stacktrace();
            }
        };
    }

    private Matcher<JsonLogLine> stacktraceMatches(String regexp) {
        return new FeatureMatcher<JsonLogLine, List<String>>(hasItems(matchesRegex(Pattern.compile(regexp, Pattern.DOTALL))),
            "error.stack_trace", "error.stack_trace") {

            @Override
            protected List<String> featureValueOf(JsonLogLine actual) {
                return actual.stacktrace();
            }
        };
    }

}

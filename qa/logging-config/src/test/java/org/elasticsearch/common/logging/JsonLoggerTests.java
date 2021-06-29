/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.logging;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configurator;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
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

    public void testDeprecatedMessageWithoutXOpaqueId() throws IOException {
        final DeprecationLogger testLogger = DeprecationLogger.getLogger("org.elasticsearch.test");

        testLogger.deprecate(DeprecationCategory.OTHER, "a key", "deprecated message1");

        final Path path = PathUtils.get(System.getProperty("es.logs.base_path"),
            System.getProperty("es.logs.cluster_name") + "_deprecated.json");

        try (Stream<Map<String, String>> stream = JsonLogsStream.mapStreamFrom(path)) {
            List<Map<String, String>> jsonLogs = stream
                .collect(Collectors.toList());

            assertThat(jsonLogs, contains(
                allOf(
                    hasEntry("event.dataset", "deprecation.elasticsearch"),
                    hasEntry("log.level", "DEPRECATION"),
                    hasEntry("log.logger", "org.elasticsearch.deprecation.test"),
                    hasEntry("elasticsearch.cluster.name", "elasticsearch"),
                    hasEntry("elasticsearch.node.name", "sample-name"),
                    hasEntry("message", "deprecated message1"),
                    hasEntry("data_stream.type", "logs"),
                    hasEntry("data_stream.dataset", "deprecation.elasticsearch"),
                    hasEntry("data_stream.namespace", "default"),
                    hasEntry("ecs.version", DeprecatedMessage.ECS_VERSION),
                    hasEntry(DeprecatedMessage.KEY_FIELD_NAME, "a key"),
                    not(hasKey(DeprecatedMessage.X_OPAQUE_ID_FIELD_NAME)),
                    hasEntry("elasticsearch.event.category", "other")
                ))
            );
        }

        assertWarnings("deprecated message1");
    }

    public void testCompatibleLog() throws Exception {
        withThreadContext(threadContext -> {
            threadContext.putHeader(Task.X_OPAQUE_ID, "someId");
            threadContext.putHeader(Task.TRACE_ID, "someTraceId");
            final DeprecationLogger testLogger = DeprecationLogger.getLogger("org.elasticsearch.test");
            testLogger.deprecate(DeprecationCategory.OTHER,"someKey", "deprecated message1")
                .compatibleApiWarning("compatibleKey","compatible API message");

            final Path path = PathUtils.get(
                System.getProperty("es.logs.base_path"),
                System.getProperty("es.logs.cluster_name") + "_deprecated.json"
            );

            try (Stream<Map<String, String>> stream = JsonLogsStream.mapStreamFrom(path)) {
                List<Map<String, String>> jsonLogs = stream.collect(Collectors.toList());

                assertThat(
                    jsonLogs,
                    contains(
                        allOf(
                            hasEntry("log.level", "DEPRECATION"),
                            hasEntry("event.dataset", "deprecation.elasticsearch"),
                            hasEntry("data_stream.dataset", "deprecation.elasticsearch"),
                            hasEntry("data_stream.namespace", "default"),
                            hasEntry("data_stream.type", "logs"),
                            hasEntry("log.logger", "org.elasticsearch.deprecation.test"),
                            hasEntry("ecs.version", DeprecatedMessage.ECS_VERSION),
                            hasEntry("elasticsearch.cluster.name", "elasticsearch"),
                            hasEntry("elasticsearch.node.name", "sample-name"),
                            hasEntry("message", "deprecated message1"),
                            hasEntry(DeprecatedMessage.KEY_FIELD_NAME, "someKey"),
                            hasEntry(DeprecatedMessage.X_OPAQUE_ID_FIELD_NAME, "someId"),
                            hasEntry(Task.TRACE_ID, "someTraceId"),
                            hasEntry("elasticsearch.event.category", "other")
                        ),
                        allOf(
                            hasEntry("log.level", "DEPRECATION"),
                            // event.dataset and data_stream.dataset have to be the same across the data stream
                            hasEntry("event.dataset", "deprecation.elasticsearch"),
                            hasEntry("data_stream.dataset", "deprecation.elasticsearch"),
                            hasEntry("data_stream.namespace", "default"),
                            hasEntry("data_stream.type", "logs"),
                            hasEntry("log.logger", "org.elasticsearch.deprecation.test"),
                            hasEntry("ecs.version", DeprecatedMessage.ECS_VERSION),
                            hasEntry("elasticsearch.cluster.name", "elasticsearch"),
                            hasEntry("elasticsearch.node.name", "sample-name"),
                            hasEntry("message", "compatible API message"),
                            hasEntry(DeprecatedMessage.KEY_FIELD_NAME, "compatibleKey"),
                            hasEntry(DeprecatedMessage.X_OPAQUE_ID_FIELD_NAME, "someId"),
                            hasEntry(Task.TRACE_ID, "someTraceId"),
                            hasEntry("elasticsearch.event.category", "compatible_api")
                        )
                    )
                );
            }

            assertWarnings("deprecated message1", "compatible API message");
        });
    }

    public void testParseFieldEmittingDeprecatedLogs() throws Exception {
        withThreadContext(threadContext -> {
            threadContext.putHeader(Task.X_OPAQUE_ID, "someId");
            threadContext.putHeader(Task.TRACE_ID, "someTraceId");

            ParseField deprecatedField = new ParseField("new_name", "deprecated_name");
            assertTrue(deprecatedField.match("deprecated_name", LoggingDeprecationHandler.INSTANCE));

            ParseField deprecatedField2 = new ParseField("new_name", "deprecated_name2");
            assertTrue(deprecatedField2.match("deprecated_name2", LoggingDeprecationHandler.INSTANCE));

            ParseField compatibleField = new ParseField("new_name", "compatible_deprecated_name")
                .forRestApiVersion(RestApiVersion.equalTo(RestApiVersion.minimumSupported()));
            assertTrue(compatibleField.match("compatible_deprecated_name", LoggingDeprecationHandler.INSTANCE));

            final Path path = PathUtils.get(
                System.getProperty("es.logs.base_path"),
                System.getProperty("es.logs.cluster_name") + "_deprecated.json"
            );

            try (Stream<Map<String, String>> stream = JsonLogsStream.mapStreamFrom(path)) {
                List<Map<String, String>> jsonLogs = stream.collect(Collectors.toList());

                assertThat(
                    jsonLogs,
                    contains(
                        // deprecation log for field deprecated_name
                        allOf(
                            hasEntry("log.level", "DEPRECATION"),
                            hasEntry("event.dataset", "deprecation.elasticsearch"),
                            hasEntry("data_stream.dataset", "deprecation.elasticsearch"),
                            hasEntry("data_stream.namespace", "default"),
                            hasEntry("data_stream.type", "logs"),
                            hasEntry("log.logger", "org.elasticsearch.deprecation.common.xcontent.ParseField"),
                            hasEntry("ecs.version", DeprecatedMessage.ECS_VERSION),
                            hasEntry("elasticsearch.cluster.name", "elasticsearch"),
                            hasEntry("elasticsearch.node.name", "sample-name"),
                            hasEntry("message", "Deprecated field [deprecated_name] used, expected [new_name] instead"),
                            hasEntry(DeprecatedMessage.KEY_FIELD_NAME, "deprecated_field_deprecated_name"),
                            hasEntry(DeprecatedMessage.X_OPAQUE_ID_FIELD_NAME, "someId"),
                            hasEntry(Task.TRACE_ID, "someTraceId"),
                            hasEntry("elasticsearch.event.category", "api")
                        ),
                        // deprecation log for field deprecated_name2 (note it is not being throttled)
                        allOf(
                            hasEntry("log.level", "DEPRECATION"),
                            hasEntry("event.dataset", "deprecation.elasticsearch"),
                            hasEntry("data_stream.dataset", "deprecation.elasticsearch"),
                            hasEntry("data_stream.namespace", "default"),
                            hasEntry("data_stream.type", "logs"),
                            hasEntry("log.logger", "org.elasticsearch.deprecation.common.xcontent.ParseField"),
                            hasEntry("ecs.version", DeprecatedMessage.ECS_VERSION),
                            hasEntry("elasticsearch.cluster.name", "elasticsearch"),
                            hasEntry("elasticsearch.node.name", "sample-name"),
                            hasEntry("message", "Deprecated field [deprecated_name2] used, expected [new_name] instead"),
                            hasEntry(DeprecatedMessage.KEY_FIELD_NAME, "deprecated_field_deprecated_name2"),
                            hasEntry(DeprecatedMessage.X_OPAQUE_ID_FIELD_NAME, "someId"),
                            hasEntry(Task.TRACE_ID, "someTraceId"),
                            hasEntry("elasticsearch.event.category", "api")
                        ),
                        // compatible log line
                        allOf(
                            hasEntry("log.level", "DEPRECATION"),
                            hasEntry("event.dataset", "deprecation.elasticsearch"),
                            hasEntry("data_stream.dataset", "deprecation.elasticsearch"),
                            hasEntry("data_stream.namespace", "default"),
                            hasEntry("data_stream.type", "logs"),
                            hasEntry("log.logger", "org.elasticsearch.deprecation.common.xcontent.ParseField"),
                            hasEntry("ecs.version", DeprecatedMessage.ECS_VERSION),
                            hasEntry("elasticsearch.cluster.name", "elasticsearch"),
                            hasEntry("elasticsearch.node.name", "sample-name"),
                            hasEntry("message", "Deprecated field [compatible_deprecated_name] used, expected [new_name] instead"),
                            hasEntry(DeprecatedMessage.KEY_FIELD_NAME, "deprecated_field_compatible_deprecated_name"),
                            hasEntry(DeprecatedMessage.X_OPAQUE_ID_FIELD_NAME, "someId"),
                            hasEntry(Task.TRACE_ID, "someTraceId"),
                            hasEntry("elasticsearch.event.category", "compatible_api")
                        )
                    )
                );
            }

            assertWarnings("Deprecated field [deprecated_name] used, expected [new_name] instead",
                "Deprecated field [deprecated_name2] used, expected [new_name] instead",
                "Deprecated field [compatible_deprecated_name] used, expected [new_name] instead");
        });
    }

    public void testDeprecatedMessage() throws Exception {
        withThreadContext(threadContext -> {
            threadContext.putHeader(Task.X_OPAQUE_ID, "someId");
            threadContext.putHeader(Task.TRACE_ID, "someTraceId");
            final DeprecationLogger testLogger = DeprecationLogger.getLogger("org.elasticsearch.test");
            testLogger.deprecate(DeprecationCategory.OTHER, "someKey", "deprecated message1");

            final Path path = PathUtils.get(
                System.getProperty("es.logs.base_path"),
                System.getProperty("es.logs.cluster_name") + "_deprecated.json"
            );

            try (Stream<Map<String, String>> stream = JsonLogsStream.mapStreamFrom(path)) {
                List<Map<String, String>> jsonLogs = stream.collect(Collectors.toList());

                assertThat(
                    jsonLogs,
                    contains(
                        allOf(
                            hasEntry("event.dataset", "deprecation.elasticsearch"),
                            hasEntry("log.level", "DEPRECATION"),
                            hasEntry("log.logger", "org.elasticsearch.deprecation.test"),
                            hasEntry("elasticsearch.cluster.name", "elasticsearch"),
                            hasEntry("elasticsearch.node.name", "sample-name"),
                            hasEntry("message", "deprecated message1"),
                            hasEntry("data_stream.type", "logs"),
                            hasEntry("data_stream.dataset", "deprecation.elasticsearch"),
                            hasEntry("data_stream.namespace", "default"),
                            hasEntry("ecs.version", DeprecatedMessage.ECS_VERSION),
                            hasEntry(DeprecatedMessage.KEY_FIELD_NAME, "someKey"),
                            hasEntry(DeprecatedMessage.X_OPAQUE_ID_FIELD_NAME, "someId"),
                            hasEntry("elasticsearch.event.category", "other")
                        )
                    )
                );
            }

            assertWarnings("deprecated message1");
        });
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
                    hasEntry("event.dataset", "elasticsearch.file"),
                    hasEntry("log.level", "INFO"),
                    hasEntry("log.logger", "test"),
                    hasEntry("elasticsearch.cluster.name", "elasticsearch"),
                    hasEntry("elasticsearch.node.name", "sample-name"),
                    hasEntry("message", "some message value0 value1"),
                    hasEntry("key1", "value1"),
                    hasEntry("key2", "value2"))
                )
            );
        }
    }

    public void testCustomMessageWithMultipleFields() throws IOException {
        // If a field is defined to be overridden, it has to always be overridden in that appender.
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
                    hasEntry("event.dataset", "elasticsearch.file"),
                    hasEntry("log.level", "INFO"),
                    hasEntry("log.logger", "test"),
                    hasEntry("elasticsearch.cluster.name", "elasticsearch"),
                    hasEntry("elasticsearch.node.name", "sample-name"),
                    hasEntry("field1", "value1"),
                    hasEntry("field2", "value2"),
                    hasEntry("message", "some message"))
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
                logLine("elasticsearch.file", Level.ERROR, "sample-name", "test", "This is an error message"),
                logLine("elasticsearch.file", Level.WARN, "sample-name", "test", "This is a warning message"),
                logLine("elasticsearch.file", Level.INFO, "sample-name", "test", "This is an info message"),
                logLine("elasticsearch.file", Level.DEBUG, "sample-name", "test", "This is a debug message"),
                logLine("elasticsearch.file", Level.TRACE, "sample-name", "test", "This is a trace message")
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
                logLine("elasticsearch.file", Level.INFO, "sample-name", "prefix.shardIdLogger",
                    "This is an info message with a shardId", Map.of(JsonLogLine::getTags, List.of("[indexName][123]"))),
                logLine("elasticsearch.file", Level.INFO, "sample-name", "prefix.prefixLogger",
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
                logLine("elasticsearch.file", Level.INFO, "sample-name", "test", json)
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
                    logLine("elasticsearch.file", Level.ERROR, "sample-name", "test", "error message"),
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
                    logLine("elasticsearch.file", Level.ERROR, "sample-name", "test", "error message " + json),

                    //stacktrace message will be single line
                    stacktraceWith("java.lang.Exception: " + json)
                )
            ));
        }
    }

    public void testDuplicateLogMessages() throws Exception {
        final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger("org.elasticsearch.test");

        // For the same key and X-Opaque-ID deprecation should be once
        withThreadContext(threadContext -> {
            threadContext.putHeader(Task.X_OPAQUE_ID, "ID1");
            deprecationLogger.deprecate(DeprecationCategory.OTHER, "key", "message1");
            deprecationLogger.deprecate(DeprecationCategory.OTHER, "key", "message2");
            assertWarnings("message1", "message2");

            final Path path = PathUtils.get(System.getProperty("es.logs.base_path"),
                System.getProperty("es.logs.cluster_name") + "_deprecated.json");
            try (Stream<Map<String, String>> stream = JsonLogsStream.mapStreamFrom(path)) {
                List<Map<String, String>> jsonLogs = stream
                    .collect(Collectors.toList());

                assertThat(jsonLogs, contains(
                    allOf(
                        hasEntry("event.dataset", "deprecation.elasticsearch"),
                        hasEntry("log.level", "DEPRECATION"),
                        hasEntry("log.logger", "org.elasticsearch.deprecation.test"),
                        hasEntry("elasticsearch.cluster.name", "elasticsearch"),
                        hasEntry("elasticsearch.node.name", "sample-name"),
                        hasEntry("message", "message1"),
                        hasEntry(DeprecatedMessage.X_OPAQUE_ID_FIELD_NAME, "ID1"),
                        hasEntry("elasticsearch.event.category", "other"))
                    )
                );
            }
        });

        // For the same key and different X-Opaque-ID should be multiple times per key/x-opaque-id
        //continuing with message1-ID1 in logs already, adding a new deprecation log line with message2-ID2
        withThreadContext(threadContext -> {
            threadContext.putHeader(Task.X_OPAQUE_ID, "ID2");
            deprecationLogger.deprecate(DeprecationCategory.OTHER, "key", "message1");
            deprecationLogger.deprecate(DeprecationCategory.OTHER, "key", "message2");
            assertWarnings("message1", "message2");

            final Path path = PathUtils.get(
                System.getProperty("es.logs.base_path"),
                System.getProperty("es.logs.cluster_name") + "_deprecated.json"
            );
            try (Stream<Map<String, String>> stream = JsonLogsStream.mapStreamFrom(path)) {
                List<Map<String, String>> jsonLogs = stream.collect(Collectors.toList());

                assertThat(
                    jsonLogs,
                    contains(
                        allOf(
                            hasEntry("event.dataset", "deprecation.elasticsearch"),
                            hasEntry("log.level", "DEPRECATION"),
                            hasEntry("log.logger", "org.elasticsearch.deprecation.test"),
                            hasEntry("elasticsearch.cluster.name", "elasticsearch"),
                            hasEntry("elasticsearch.node.name", "sample-name"),
                            hasEntry("message", "message1"),
                            hasEntry(DeprecatedMessage.X_OPAQUE_ID_FIELD_NAME, "ID1"),
                            hasEntry("elasticsearch.event.category", "other")
                        ),
                        allOf(
                            hasEntry("event.dataset", "deprecation.elasticsearch"),
                            hasEntry("log.level", "DEPRECATION"),
                            hasEntry("log.logger", "org.elasticsearch.deprecation.test"),
                            hasEntry("elasticsearch.cluster.name", "elasticsearch"),
                            hasEntry("elasticsearch.node.name", "sample-name"),
                            hasEntry("message", "message1"),
                            hasEntry(DeprecatedMessage.X_OPAQUE_ID_FIELD_NAME, "ID2"),
                            hasEntry("elasticsearch.event.category", "other")
                        )
                    )
                );
            }
        });
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
        return Map.of(JsonLogLine::getDataset, type,
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

    private void withThreadContext(CheckedConsumer<ThreadContext, Exception> consumer) throws Exception {
        final ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
            HeaderWarning.setThreadContext(threadContext);
            consumer.accept(threadContext);
        } finally {
            HeaderWarning.removeThreadContext(threadContext);
        }
    }
}

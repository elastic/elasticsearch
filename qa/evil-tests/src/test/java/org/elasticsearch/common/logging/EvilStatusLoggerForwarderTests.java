/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.logging;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.core.config.Property;
import org.apache.logging.log4j.status.StatusConsoleListener;
import org.apache.logging.log4j.status.StatusData;
import org.apache.logging.log4j.status.StatusListener;
import org.apache.logging.log4j.status.StatusLogger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.plugins.internal.LoggingDataProvider;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.test.LambdaMatchers.transformedMatch;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;

public class EvilStatusLoggerForwarderTests extends ESTestCase {

    private static final int MAX_FAILING_CALLS = 200;

    private static final AtomicInteger failingCalls = new AtomicInteger();
    private static volatile Thread failingThread;
    private static volatile boolean throwError;

    /**
     * Data providers can only be set once per JVM, so install a single failing provider followed by a healthy one. The failing
     * provider only fails on the triggering test thread, so background logging cannot affect the call counts.
     */
    @BeforeClass
    public static void installDataProviders() {
        LoggingDataProvider failingProvider = data -> {
            if (Thread.currentThread() == failingThread && failingCalls.incrementAndGet() <= MAX_FAILING_CALLS) {
                if (throwError) {
                    throw new AssertionError("simulated provider error");
                }
                throw new IllegalStateException("simulated provider failure");
            }
        };
        LoggingDataProvider healthyProvider = data -> data.put("healthy", "value");
        DynamicContextDataProvider.setDataProviders(List.of(failingProvider, healthyProvider));
    }

    @Before
    public void registerErrorListener() {
        LogConfigurator.registerErrorListener();
    }

    @After
    public void shutdownLogging() {
        LoggerContext context = (LoggerContext) LogManager.getContext(false);
        Configurator.shutdown(context);
    }

    public void testFailingLoggingDataProviderDoesNotDropEvents() throws IOException {
        List<String> messages = randomList(1, 5, () -> randomAlphaOfLength(10));
        Captured captured = triggerWithFailingProvider(false, messages.toArray(String[]::new));

        // Each event fails once itself and once more when the forwarder logs the resulting status warning
        assertThat(failingCalls.get(), equalTo(2 * messages.size()));
        assertThat(captured.warnings(), hasSize(2 * messages.size()));
        assertThat(
            captured.warnings().stream().map(w -> w.getMessage().getFormattedMessage()).toList(),
            everyItem(containsString("logging data provider"))
        );
        assertThat(captured.events().stream().map(e -> e.getMessage().getFormattedMessage()).toList(), equalTo(messages));
        assertThat(captured.events(), everyItem(transformedMatch(e -> e.getContextData().getValue("healthy"), equalTo("value"))));
    }

    public void testErrorEscapingLoggingDataProviderTerminates() throws IOException {
        // Errors are not caught by DynamicContextDataProvider, so they reach log4j and the StatusLogger forwarder
        Captured captured = triggerWithFailingProvider(true, "trigger");

        assertThat("original call and one forwarded status event", failingCalls.get(), equalTo(2));
        assertThat(captured.warnings(), hasSize(2));
        for (StatusData warning : captured.warnings()) {
            assertThat(warning.getMessage().getFormattedMessage(), containsString("caught java.lang.AssertionError"));
            assertThat(
                "re-entrant status events must still reach the console",
                captured.console(),
                containsString(warning.getMessage().getFormattedMessage())
            );
        }
    }

    private record Captured(List<LogEvent> events, List<StatusData> warnings, String console) {}

    private Captured triggerWithFailingProvider(boolean error, String... messages) throws IOException {
        int configurations = randomIntBetween(1, 3);
        for (int i = 0; i < configurations; i++) {
            LogConfigurator.registerErrorListener();
            setupLogging("minimal");
        }

        List<StatusListener> others = new ArrayList<>();
        List<StatusListener> forwarders = new ArrayList<>();
        StatusLogger.getLogger().getListeners().forEach(l -> (isForwarder(l) ? forwarders : others).add(l));
        assertThat("reconfiguring must replace the previous forwarder", forwarders, hasSize(1));
        assertThat(forwarders.get(0), instanceOf(StatusConsoleListener.class));
        StatusConsoleListener forwarder = (StatusConsoleListener) forwarders.get(0);
        ByteArrayOutputStream console = new ByteArrayOutputStream();
        forwarder.setStream(new PrintStream(console, true, StandardCharsets.UTF_8));

        List<StatusData> warnings = new CopyOnWriteArrayList<>();
        StatusListener capturing = new StatusListener() {
            @Override
            public void log(StatusData data) {
                warnings.add(data);
            }

            @Override
            public Level getStatusLevel() {
                return Level.WARN;
            }

            @Override
            public void close() {}
        };

        List<LogEvent> events = new CopyOnWriteArrayList<>();
        AbstractAppender appender = new AbstractAppender("capture", null, null, false, Property.EMPTY_ARRAY) {
            @Override
            public void append(LogEvent event) {
                events.add(event.toImmutable());
            }
        };
        appender.start();
        Logger testLogger = LogManager.getLogger("test");
        Loggers.addAppender(testLogger, appender);

        // ESTestCase fails any test that emits StatusLogger warnings, which this test does on purpose.
        others.forEach(StatusLogger.getLogger()::removeListener);
        StatusLogger.getLogger().registerListener(capturing);
        failingCalls.set(0);
        throwError = error;
        failingThread = Thread.currentThread();
        try {
            for (String message : messages) {
                testLogger.info(message);
            }
        } finally {
            failingThread = null;
            Loggers.removeAppender(testLogger, appender);
            StatusLogger.getLogger().removeListener(capturing);
            StatusLogger.getLogger().removeListener(forwarder);
            others.forEach(StatusLogger.getLogger()::registerListener);
        }
        return new Captured(events, warnings, console.toString(StandardCharsets.UTF_8));
    }

    private static boolean isForwarder(StatusListener listener) {
        var enclosingMethod = listener.getClass().getEnclosingMethod();
        return enclosingMethod != null
            && enclosingMethod.getDeclaringClass() == LogConfigurator.class
            && enclosingMethod.getName().equals("configureStatusLoggerForwarder");
    }

    private void setupLogging(final String config) throws IOException {
        final Path configDir = getDataPath(config);
        final Settings settings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString()).build();
        LogConfigurator.configure(new Environment(settings, configDir), true);
    }
}

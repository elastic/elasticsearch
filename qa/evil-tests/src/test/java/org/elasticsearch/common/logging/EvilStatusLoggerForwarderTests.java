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
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.status.StatusData;
import org.apache.logging.log4j.status.StatusListener;
import org.apache.logging.log4j.status.StatusLogger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.lessThan;

public class EvilStatusLoggerForwarderTests extends ESTestCase {

    // Safety cap so the test terminates even if the forwarder never stops re-entering.
    private static final int MAX_FAILING_CALLS = 200;

    private static final AtomicReference<Supplier<Throwable>> failure = new AtomicReference<>();
    private static final AtomicInteger failingCalls = new AtomicInteger();

    @Before
    public void registerErrorListener() {
        LogConfigurator.registerErrorListener();
    }

    @After
    public void shutdownLogging() {
        failure.set(null);
        LoggerContext context = (LoggerContext) LogManager.getContext(false);
        Configurator.shutdown(context);
    }

    public void testFailingLoggingDataProviderIsReportedOnce() throws IOException {
        List<StatusData> warnings = triggerWithFailingProvider(() -> new IllegalStateException("simulated provider failure"));

        assertThat("provider must be invoked by log4j", failingCalls.get(), greaterThan(0));
        assertThat(warnings, hasSize(1));
        assertThat(warnings.get(0).getMessage().getFormattedMessage(), containsString("logging data provider"));
    }

    public void testErrorEscapingLoggingDataProviderTerminates() throws IOException {
        // Errors are not caught by DynamicContextDataProvider, so they reach log4j and the StatusLogger forwarder
        triggerWithFailingProvider(() -> new AssertionError("simulated provider error"));

        assertThat("provider must be invoked by log4j", failingCalls.get(), greaterThan(0));
        assertThat("must stop failing before the safety cap", failingCalls.get(), lessThan(MAX_FAILING_CALLS));
    }

    private List<StatusData> triggerWithFailingProvider(Supplier<Throwable> failureSupplier) throws IOException {
        DynamicContextDataProvider.setDataProviders(List.of(data -> {
            Supplier<Throwable> current = failure.get();
            if (current != null && failingCalls.incrementAndGet() <= MAX_FAILING_CALLS) {
                Throwable t = current.get();
                if (t instanceof RuntimeException e) {
                    throw e;
                }
                throw (Error) t;
            }
        }));
        List<StatusListener> preExisting = new ArrayList<>();
        StatusLogger.getLogger().getListeners().forEach(preExisting::add);
        setupLogging("minimal");

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

        // ESTestCase fails any test that emits StatusLogger warnings, which this test does on purpose.
        preExisting.forEach(StatusLogger.getLogger()::removeListener);
        StatusLogger.getLogger().registerListener(capturing);
        failingCalls.set(0);
        failure.set(failureSupplier);
        try {
            LogManager.getLogger("test").info("trigger");
        } finally {
            failure.set(null);
            StatusLogger.getLogger().removeListener(capturing);
            preExisting.forEach(StatusLogger.getLogger()::registerListener);
        }
        return warnings;
    }

    private void setupLogging(final String config) throws IOException {
        final Path configDir = getDataPath(config);
        final Settings settings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString()).build();
        LogConfigurator.configure(new Environment(settings, configDir), true);
    }
}

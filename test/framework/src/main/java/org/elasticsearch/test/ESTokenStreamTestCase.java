/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test;

import com.carrotsearch.randomizedtesting.annotations.Listeners;
import com.carrotsearch.randomizedtesting.annotations.TimeoutSuite;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.lucene.tests.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TimeUnits;
import org.elasticsearch.bootstrap.BootstrapForTesting;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.common.logging.HeaderWarningAppender;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.test.junit.listeners.ReproduceInfoPrinter;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasItem;

@Listeners({ ReproduceInfoPrinter.class })
@TimeoutSuite(millis = TimeUnits.HOUR)
@LuceneTestCase.SuppressReproduceLine
@LuceneTestCase.SuppressSysoutChecks(bugUrl = "we log a lot on purpose")
/**
 * Basic test case for token streams. the assertion methods in this class will
 * run basic checks to enforce correct behavior of the token streams.
 */
public abstract class ESTokenStreamTestCase extends BaseTokenStreamTestCase {

    static {
        try {
            Class.forName("org.elasticsearch.test.ESTestCase");
        } catch (ClassNotFoundException e) {
            throw new AssertionError(e);
        }
        BootstrapForTesting.ensureInitialized();
    }

    public Settings.Builder newAnalysisSettingsBuilder() {
        return Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current());
    }

    private HeaderWarningAppender headerWarningAppender;
    private ThreadContext threadContext;

    @Before
    public final void before() {
        this.headerWarningAppender = HeaderWarningAppender.createAppender("header_warning", null);
        this.headerWarningAppender.start();
        Loggers.addAppender(LogManager.getLogger("org.elasticsearch.deprecation"), this.headerWarningAppender);
        assertNull("Thread context initialized twice", threadContext);
        if (enableWarningsCheck()) {
            this.threadContext = new ThreadContext(Settings.EMPTY);
            HeaderWarning.setThreadContext(threadContext);
        }
    }

    @After
    public final void after() throws Exception {
        // We check threadContext != null rather than enableWarningsCheck()
        // because after methods are still called in the event that before
        // methods failed, in which case threadContext might not have been
        // initialized
        if (threadContext != null) {
            ensureNoWarnings();
            HeaderWarning.removeThreadContext(threadContext);
            threadContext = null;
        }
        if (this.headerWarningAppender != null) {
            Loggers.removeAppender(LogManager.getLogger("org.elasticsearch.deprecation"), this.headerWarningAppender);
            this.headerWarningAppender = null;
        }
    }

    /**
     * Whether or not we check after each test whether it has left warnings behind. That happens if any deprecated feature or syntax
     * was used by the test and the test didn't assert on it using {@link #assertWarnings(String...)}.
     */
    protected boolean enableWarningsCheck() {
        return true;
    }

    protected List<String> filteredWarnings() {
        List<String> filtered = new ArrayList<>();
        filtered.add(
            "Configuring multiple [path.data] paths is deprecated. Use RAID or other system level features for utilizing"
                + " multiple disks. This feature will be removed in a future release."
        );
        filtered.add("Configuring [path.data] with a list is deprecated. Instead specify as a string value");
        filtered.add("setting [path.shared_data] is deprecated and will be removed in a future release");
        return filtered;
    }

    /**
     * Convenience method to assert warnings for settings deprecations and general deprecation warnings. All warnings passed to this method
     * are assumed to be at WARNING level.
     * @param expectedWarnings expected general deprecation warning messages.
     */
    protected final void assertWarnings(String... expectedWarnings) {
        assertWarnings(
            true,
            Arrays.stream(expectedWarnings)
                .map(expectedWarning -> new ESTestCase.DeprecationWarning(Level.WARN, expectedWarning))
                .toArray(ESTestCase.DeprecationWarning[]::new)
        );
    }

    protected final void assertWarnings(boolean stripXContentPosition, ESTestCase.DeprecationWarning... expectedWarnings) {
        if (enableWarningsCheck() == false) {
            throw new IllegalStateException("unable to check warning headers if the test is not set to do so");
        }
        try {
            final List<String> actualWarningStrings = threadContext.getResponseHeaders().get("Warning");
            if (expectedWarnings == null || expectedWarnings.length == 0) {
                assertNull("expected 0 warnings, actual: " + actualWarningStrings, actualWarningStrings);
            } else {
                assertNotNull("no warnings, expected: " + Arrays.asList(expectedWarnings), actualWarningStrings);
                final Set<ESTestCase.DeprecationWarning> actualDeprecationWarnings = actualWarningStrings.stream().map(warningString -> {
                    String warningText = HeaderWarning.extractWarningValueFromWarningHeader(warningString, stripXContentPosition);
                    final Level level;
                    if (warningString.startsWith(Integer.toString(DeprecationLogger.CRITICAL.intLevel()))) {
                        level = DeprecationLogger.CRITICAL;
                    } else if (warningString.startsWith(Integer.toString(Level.WARN.intLevel()))) {
                        level = Level.WARN;
                    } else {
                        throw new IllegalArgumentException("Unknown level in deprecation message " + warningString);
                    }
                    return new ESTestCase.DeprecationWarning(level, warningText);
                }).collect(Collectors.toSet());
                for (ESTestCase.DeprecationWarning expectedWarning : expectedWarnings) {
                    ESTestCase.DeprecationWarning escapedExpectedWarning = new ESTestCase.DeprecationWarning(
                        expectedWarning.level(),
                        HeaderWarning.escapeAndEncode(expectedWarning.message())
                    );
                    assertThat(actualDeprecationWarnings, hasItem(escapedExpectedWarning));
                }
                assertEquals(
                    "Expected "
                        + expectedWarnings.length
                        + " warnings but found "
                        + actualWarningStrings.size()
                        + "\nExpected: "
                        + Arrays.asList(expectedWarnings)
                        + "\nActual: "
                        + actualWarningStrings,
                    expectedWarnings.length,
                    actualWarningStrings.size()
                );
            }
        } finally {
            resetDeprecationLogger();
        }
    }

    public void ensureNoWarnings() {
        // Check that there are no unaccounted warning headers. These should be checked with {@link #assertWarnings(String...)} in the
        // appropriate test
        try {
            final List<String> warnings = threadContext.getResponseHeaders().get("Warning");
            if (warnings != null) {
                // unit tests do not run with the bundled JDK, if there are warnings we need to filter the no-jdk deprecation warning
                final List<String> filteredWarnings = warnings.stream()
                    .filter(k -> filteredWarnings().stream().noneMatch(s -> k.contains(s)))
                    .collect(Collectors.toList());
                assertThat("unexpected warning headers", filteredWarnings, empty());
            } else {
                assertNull("unexpected warning headers", warnings);
            }
        } finally {
            resetDeprecationLogger();
        }
    }

    /**
     * Reset the deprecation logger by clearing the current thread context.
     */
    private void resetDeprecationLogger() {
        // "clear" context by stashing current values and dropping the returned StoredContext
        threadContext.stashContext();
    }
}

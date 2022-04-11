/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authz;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLogAppender;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine;
import org.elasticsearch.xpack.core.security.user.User;
import org.hamcrest.Matchers;
import org.junit.Before;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import static org.elasticsearch.test.TestMatchers.throwableWithMessage;

public class LoadAuthorizedIndicesTimeCheckerTests extends ESTestCase {

    private Logger timerLogger;
    private ClusterSettings clusterSettings;

    @Before
    public void setUpLogger() throws Exception {
        this.timerLogger = LogManager.getLogger(getClass().getPackageName() + ".timer");
    }

    public void testResolveThresholdSettings() {
        final Settings settings = Settings.builder()
            .put("xpack.security.authz.timer.indices.threshold.debug", TimeValue.timeValueSeconds(1))
            .put("xpack.security.authz.timer.indices.threshold.info", TimeValue.timeValueMinutes(2))
            .put("xpack.security.authz.timer.indices.threshold.warn", TimeValue.timeValueHours(3))
            .build();
        final LoadAuthorizedIndicesTimeChecker.Factory factory = buildFactory(settings);
        final LoadAuthorizedIndicesTimeChecker.Thresholds thresholds = factory.getThresholds();

        assertThat(thresholds, Matchers.notNullValue());
        assertThat(thresholds.getDebugThresholdMs(), Matchers.is(1000L));
        assertThat(thresholds.getInfoThresholdMs(), Matchers.is(2 * 60 * 1000L));
        assertThat(thresholds.getWarnThresholdMs(), Matchers.is(3 * 60 * 60 * 1000L));
    }

    public void testValidateThresholdSettings() {
        // Randomly set (info < debug), or (warn < info)
        final boolean invalidDebugVsInfo = randomBoolean();
        final Settings.Builder builder = Settings.builder();
        if (invalidDebugVsInfo) {
            builder.put("xpack.security.authz.timer.indices.threshold.debug", TimeValue.timeValueMillis(60))
                .put("xpack.security.authz.timer.indices.threshold.info", TimeValue.timeValueMillis(50));
        } else {
            builder.put("xpack.security.authz.timer.indices.threshold.info", TimeValue.timeValueMillis(50))
                .put("xpack.security.authz.timer.indices.threshold.warn", TimeValue.timeValueMillis(40));
        }
        final Settings settings = builder.build();
        final SettingsException exception = expectThrows(SettingsException.class, () -> buildFactory(settings));
        if (invalidDebugVsInfo) {
            assertThat(
                exception,
                throwableWithMessage(
                    "Setting [xpack.security.authz.timer.indices.threshold.info] (50ms)"
                        + " cannot be less than the setting [xpack.security.authz.timer.indices.threshold.debug] (60ms)"
                )
            );
        } else {
            assertThat(
                exception,
                throwableWithMessage(
                    "Setting [xpack.security.authz.timer.indices.threshold.warn] (40ms)"
                        + " cannot be less than the setting [xpack.security.authz.timer.indices.threshold.info] (50ms)"
                )
            );
        }
    }

    public void testResolveDisabledLogging() {
        final Settings settings = randomBoolean()
            ? Settings.EMPTY
            : Settings.builder().put("xpack.security.authz.timer.indices.enabled", false).build();
        final LoadAuthorizedIndicesTimeChecker.Factory factory = buildFactory(settings);
        assertThat(factory.newTimer(null), Matchers.is(LoadAuthorizedIndicesTimeChecker.NO_OP_CONSUMER));
    }

    public void testResolveEnabledLogging() {
        final Settings settings = Settings.builder().put("xpack.security.authz.timer.indices.enabled", true).build();
        final LoadAuthorizedIndicesTimeChecker.Factory factory = buildFactory(settings);
        assertThat(factory.newTimer(null), Matchers.instanceOf(LoadAuthorizedIndicesTimeChecker.class));
    }

    public void testDynamicallyEnableLogging() throws Exception {
        Settings settings = randomBoolean()
            ? Settings.EMPTY
            : Settings.builder().put("xpack.security.authz.timer.indices.enabled", false).build();
        final LoadAuthorizedIndicesTimeChecker.Factory factory = buildFactory(settings);

        assertThat(factory.newTimer(null), Matchers.is(LoadAuthorizedIndicesTimeChecker.NO_OP_CONSUMER));

        settings = Settings.builder().put("xpack.security.authz.timer.indices.enabled", true).build();
        clusterSettings.applySettings(settings);

        assertThat(factory.newTimer(null), Matchers.instanceOf(LoadAuthorizedIndicesTimeChecker.class));

        settings = randomBoolean() ? Settings.EMPTY : Settings.builder().put("xpack.security.authz.timer.indices.enabled", false).build();
        clusterSettings.applySettings(settings);

        assertThat(factory.newTimer(null), Matchers.is(LoadAuthorizedIndicesTimeChecker.NO_OP_CONSUMER));
    }

    public void testWarning() throws Exception {
        final int warnMs = randomIntBetween(150, 500);
        final LoadAuthorizedIndicesTimeChecker.Thresholds thresholds = new LoadAuthorizedIndicesTimeChecker.Thresholds(
            TimeValue.timeValueMillis(randomIntBetween(1, 99)),
            TimeValue.timeValueMillis(randomIntBetween(100, warnMs - 1)),
            TimeValue.timeValueMillis(warnMs)
        );
        final int elapsedMs = warnMs + randomIntBetween(1, 100);

        final MockLogAppender.PatternSeenEventExpectation expectation = new MockLogAppender.PatternSeenEventExpectation(
            "WARN-Slow Index Resolution",
            timerLogger.getName(),
            Level.WARN,
            Pattern.quote("Resolving [0] indices for action [" + SearchAction.NAME + "] and user [slow-user] took [")
                + "\\d{3}"
                + Pattern.quote(
                    "ms] which is greater than the threshold of "
                        + warnMs
                        + "ms;"
                        + " The index privileges for this user may be too complex for this cluster."
                )
        );

        testLogging(thresholds, elapsedMs, expectation);
    }

    public void testInfo() throws Exception {
        final int infoMs = randomIntBetween(50, 150);
        final LoadAuthorizedIndicesTimeChecker.Thresholds thresholds = new LoadAuthorizedIndicesTimeChecker.Thresholds(
            TimeValue.timeValueMillis(randomIntBetween(1, infoMs - 1)),
            TimeValue.timeValueMillis(infoMs),
            TimeValue.timeValueHours(1) // Very long so we know we only get info logs
        );
        final int elapsedMs = infoMs + randomIntBetween(1, 100);

        final MockLogAppender.PatternSeenEventExpectation expectation = new MockLogAppender.PatternSeenEventExpectation(
            "INFO-Slow Index Resolution",
            timerLogger.getName(),
            Level.INFO,
            Pattern.quote("Took [")
                + "\\d{2,3}"
                + Pattern.quote("ms] to resolve [0] indices for action [" + SearchAction.NAME + "] and user [slow-user]")
        );

        testLogging(thresholds, elapsedMs, expectation);
    }

    private void testLogging(
        LoadAuthorizedIndicesTimeChecker.Thresholds thresholds,
        int elapsedMs,
        MockLogAppender.PatternSeenEventExpectation expectation
    ) throws IllegalAccessException {
        final User user = new User("slow-user", "slow-role");
        final Authentication authentication = new Authentication(user, new Authentication.RealmRef("test", "test", "foo"), null);
        final AuthorizationEngine.RequestInfo requestInfo = new AuthorizationEngine.RequestInfo(
            authentication,
            new SearchRequest(),
            SearchAction.NAME,
            null
        );

        final Logger timerLogger = LogManager.getLogger(getClass().getPackageName() + ".timer");
        final LoadAuthorizedIndicesTimeChecker checker = new LoadAuthorizedIndicesTimeChecker(
            timerLogger,
            System.nanoTime() - TimeUnit.MILLISECONDS.toNanos(elapsedMs),
            requestInfo,
            thresholds
        );
        final MockLogAppender mockAppender = new MockLogAppender();
        mockAppender.start();
        try {
            Loggers.addAppender(timerLogger, mockAppender);
            mockAppender.addExpectation(expectation);
            checker.accept(List.of());
            mockAppender.assertAllExpectationsMatched();
        } finally {
            Loggers.removeAppender(timerLogger, mockAppender);
            mockAppender.stop();
        }
    }

    private LoadAuthorizedIndicesTimeChecker.Factory buildFactory(Settings settings) {
        this.clusterSettings = new ClusterSettings(settings, LoadAuthorizedIndicesTimeChecker.Factory.getSettings());
        return new LoadAuthorizedIndicesTimeChecker.Factory(logger, settings, clusterSettings);
    }

}

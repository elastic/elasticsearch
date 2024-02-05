/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.junit.listeners;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.logging.TestLoggers;
import org.elasticsearch.test.junit.annotations.TestIssueLogging;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.junit.runner.Description;
import org.junit.runner.Result;
import org.junit.runner.notification.RunListener;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * A {@link RunListener} that allows changing the log level for a specific test method. When a test method is annotated with the
 * {@link TestLogging} annotation, the level for the specified loggers will be internally saved before the test method execution and
 * overridden with the specified ones. At the end of the test method execution the original loggers levels will be restored.
 *
 * This class is not thread-safe. Given the static nature of the logging API, it assumes that tests are never run concurrently in the same
 * JVM. For the very same reason no synchronization has been implemented regarding the save/restore process of the original loggers
 * levels.
 */
public class LoggingListener extends RunListener {

    private Map<Logger, Level> previousLoggingMap;
    private Map<Logger, Level> previousClassLoggingMap;
    private Map<Logger, Level> previousPackageLoggingMap;

    @Override
    public void testRunStarted(final Description description) throws Exception {
        Package testClassPackage = description.getTestClass().getPackage();
        previousPackageLoggingMap = processTestLogging(
            testClassPackage != null ? testClassPackage.getAnnotation(TestLogging.class) : null,
            testClassPackage != null ? testClassPackage.getAnnotation(TestIssueLogging.class) : null
        );
        previousClassLoggingMap = processTestLogging(
            description.getAnnotation(TestLogging.class),
            description.getAnnotation(TestIssueLogging.class)
        );
    }

    @Override
    public void testRunFinished(final Result result) throws Exception {
        previousClassLoggingMap = reset(previousClassLoggingMap);
        previousPackageLoggingMap = reset(previousPackageLoggingMap);
    }

    @Override
    public void testStarted(final Description description) throws Exception {
        final TestLogging testLogging = description.getAnnotation(TestLogging.class);
        final TestIssueLogging testIssueLogging = description.getAnnotation(TestIssueLogging.class);
        previousLoggingMap = processTestLogging(testLogging, testIssueLogging);
    }

    @Override
    public void testFinished(final Description description) throws Exception {
        previousLoggingMap = reset(previousLoggingMap);
    }

    /**
     * Obtain the logger with the given name.
     *
     * @param loggerName the logger to obtain
     * @return the logger
     */
    private static Logger resolveLogger(String loggerName) {
        if (loggerName.equalsIgnoreCase("_root")) {
            return LogManager.getRootLogger();
        }
        return LogManager.getLogger(loggerName);
    }

    /**
     * Applies the test logging annotation and returns the existing logging levels.
     *
     * @param testLogging the test logging annotation to apply
     * @return the existing logging levels
     */
    private SortedMap<Logger, Level> processTestLogging(final TestLogging testLogging, final TestIssueLogging testIssueLogging) {
        final Map<Logger, Level> testLoggingMap = getLoggersAndLevelsFromAnnotation(testLogging);
        final Map<Logger, Level> testIssueLoggingMap = getLoggersAndLevelsFromAnnotation(testIssueLogging);

        final Set<Logger> testLoggingKeys = new HashSet<>(testLoggingMap.keySet());
        testLoggingKeys.retainAll(testIssueLoggingMap.keySet());
        if (testLoggingKeys.isEmpty() == false) {
            List<String> duplicates = testLoggingKeys.stream().map(Logger::getName).toList();
            throw new IllegalArgumentException("found intersection " + duplicates + " between TestLogging and TestIssueLogging");
        }

        /*
         * Use a sorted set so that we apply a parent logger before its children thus not overwriting the child setting when processing the
         * parent setting.
         */
        final SortedMap<Logger, Level> loggingLevels = new TreeMap<>(loggerComparator());
        loggingLevels.putAll(testLoggingMap);
        loggingLevels.putAll(testIssueLoggingMap);

        /*
         * Obtain the existing logging levels so that we can restore them at the end of the test. We have to do this separately from
         * setting the logging levels so that setting foo does not impact the logging level for for.bar when we check the existing logging
         * level for foo.bar.
         */
        final SortedMap<Logger, Level> existing = new TreeMap<>(loggingLevels.comparator());
        for (final Logger logger : loggingLevels.keySet()) {
            existing.put(logger, logger.getLevel());
        }
        loggingLevels.forEach(TestLoggers::setLevel);
        return existing;
    }

    /**
     * Obtain the logging levels from the test logging annotation.
     *
     * @param testLogging the test logging annotation
     * @return a map from logger name to logging level
     */
    private static Map<Logger, Level> getLoggersAndLevelsFromAnnotation(final TestLogging testLogging) {
        if (testLogging == null) {
            return Collections.emptyMap();
        }

        return getLoggersAndLevelsFromAnnotationValue(testLogging.value());
    }

    private static Map<Logger, Level> getLoggersAndLevelsFromAnnotation(final TestIssueLogging testIssueLogging) {
        if (testIssueLogging == null) {
            return Collections.emptyMap();
        }

        return getLoggersAndLevelsFromAnnotationValue(testIssueLogging.value());
    }

    private static Map<Logger, Level> getLoggersAndLevelsFromAnnotationValue(final String value) {
        /*
         * Use a sorted set so that we apply a parent logger before its children thus not overwriting the child setting when processing the
         * parent setting.
         */
        final Map<Logger, Level> map = new TreeMap<>(loggerComparator());
        final String[] loggersAndLevels = value.split(",");
        for (final String loggerAndLevel : loggersAndLevels) {
            final String[] loggerAndLevelArray = loggerAndLevel.split(":");
            if (loggerAndLevelArray.length == 2) {
                Logger logger = resolveLogger(loggerAndLevelArray[0]);
                Level level = loggerAndLevelArray[1] != null ? Level.valueOf(loggerAndLevelArray[1]) : null;
                map.put(logger, level);
            } else {
                throw new IllegalArgumentException("invalid test logging annotation [" + loggerAndLevel + "]");
            }
        }
        return map;
    }

    /**
     * Reset the logging levels to the state provided by the map.
     *
     * @param map the logging levels to apply
     * @return an empty map
     */
    private Map<Logger, Level> reset(final Map<Logger, Level> map) {
        map.forEach(TestLoggers::setLevel);
        return Collections.emptyMap();
    }

    private static Comparator<Logger> loggerComparator() {
        return Comparator.comparing(Logger::getName);
    }
}

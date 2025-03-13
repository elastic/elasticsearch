/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.junit.listeners;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.test.junit.annotations.TestIssueLogging;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.junit.runner.Description;
import org.junit.runner.Result;
import org.junit.runner.notification.RunListener;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Stream;

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

    private Map<String, String> previousLoggingMap;
    private Map<String, String> previousClassLoggingMap;
    private Map<String, String> previousPackageLoggingMap;

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
    private Map<String, String> processTestLogging(final TestLogging testLogging, final TestIssueLogging testIssueLogging) {
        final Map<String, String> testLoggingMap = getLoggersAndLevelsFromAnnotation(testLogging);
        final Map<String, String> testIssueLoggingMap = getLoggersAndLevelsFromAnnotation(testIssueLogging);

        final Set<String> testLoggingKeys = new HashSet<>(testLoggingMap.keySet());
        testLoggingKeys.retainAll(testIssueLoggingMap.keySet());
        if (testLoggingKeys.isEmpty() == false) {
            throw new IllegalArgumentException("found intersection " + testLoggingKeys + " between TestLogging and TestIssueLogging");
        }

        /*
         * Use a sorted set so that we apply a parent logger before its children thus not overwriting the child setting when processing the
         * parent setting.
         */
        final Map<String, String> loggingLevels = Stream.concat(testLoggingMap.entrySet().stream(), testIssueLoggingMap.entrySet().stream())
            .collect(Maps.toUnmodifiableSortedMap(Map.Entry::getKey, Map.Entry::getValue));

        /*
         * Obtain the existing logging levels so that we can restore them at the end of the test. We have to do this separately from
         * setting the logging levels so that setting foo does not impact the logging level for for.bar when we check the existing logging
         * level for foo.bar.
         */
        final Map<String, String> existing = new TreeMap<>();
        for (final Map.Entry<String, String> entry : loggingLevels.entrySet()) {
            final Logger logger = resolveLogger(entry.getKey());
            existing.put(entry.getKey(), logger.getLevel().toString());
        }
        for (final Map.Entry<String, String> entry : loggingLevels.entrySet()) {
            final Logger logger = resolveLogger(entry.getKey());
            Loggers.setLevel(logger, entry.getValue());
        }
        return existing;
    }

    /**
     * Obtain the logging levels from the test logging annotation.
     *
     * @param testLogging the test logging annotation
     * @return a map from logger name to logging level
     */
    private static Map<String, String> getLoggersAndLevelsFromAnnotation(final TestLogging testLogging) {
        if (testLogging == null) {
            return Collections.emptyMap();
        }

        return getLoggersAndLevelsFromAnnotationValue(testLogging.value());
    }

    private static Map<String, String> getLoggersAndLevelsFromAnnotation(final TestIssueLogging testIssueLogging) {
        if (testIssueLogging == null) {
            return Collections.emptyMap();
        }

        return getLoggersAndLevelsFromAnnotationValue(testIssueLogging.value());
    }

    private static Map<String, String> getLoggersAndLevelsFromAnnotationValue(final String value) {
        /*
         * Use a sorted set so that we apply a parent logger before its children thus not overwriting the child setting when processing the
         * parent setting.
         */
        final Map<String, String> map = new TreeMap<>();
        final String[] loggersAndLevels = value.split(",");
        for (final String loggerAndLevel : loggersAndLevels) {
            final String[] loggerAndLevelArray = loggerAndLevel.split(":");
            if (loggerAndLevelArray.length == 2) {
                map.put(loggerAndLevelArray[0], loggerAndLevelArray[1]);
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
    private Map<String, String> reset(final Map<String, String> map) {
        for (final Map.Entry<String, String> previousLogger : map.entrySet()) {
            final Logger logger = resolveLogger(previousLogger.getKey());
            Loggers.setLevel(logger, previousLogger.getValue());
        }

        return Collections.emptyMap();
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import java.util.HashSet;
import java.util.Set;

import static org.elasticsearch.common.logging.HeaderWarning.addWarning;
import static org.elasticsearch.common.logging.LoggerMessageFormat.format;

/**
 * Utilities to collect warnings for running an executor.
 */
public class Warnings {
    static final int MAX_ADDED_WARNINGS = 20;

    public static final Warnings NOOP_WARNINGS = new Warnings(-1, -2, null, "", "") {
        @Override
        public void registerException(Exception exception) {
            // this space intentionally left blank
        }

        @Override
        public void registerWarning(String message) {
            // this space intentionally left blank
        }
    };

    /**
     * Create a new warnings object based on the given mode
     * @param warningsMode The warnings collection strategy to use
     * @param source The source location information for warnings
     * @return A warnings collector object
     */
    public static Warnings createWarnings(DriverContext.WarningsMode warningsMode, WarningSourceLocation source) {
        return createWarnings(warningsMode, source, "evaluation of [{}] failed, treating result as null");
    }

    /**
     * Create a new warnings object based on the given mode which warns that
     * it treats the result as {@code false}.
     * @param warningsMode The warnings collection strategy to use
     * @param source The source location information for warnings
     * @return A warnings collector object
     */
    public static Warnings createWarningsTreatedAsFalse(DriverContext.WarningsMode warningsMode, WarningSourceLocation source) {
        return createWarnings(warningsMode, source, "evaluation of [{}] failed, treating result as false");
    }

    /**
     * Create a new warnings object based on the given mode which warns that
     * evaluation resulted in warnings.
     * @param warningsMode The warnings collection strategy to use
     * @param source The source location information for warnings
     * @return A warnings collector object
     */
    public static Warnings createOnlyWarnings(DriverContext.WarningsMode warningsMode, WarningSourceLocation source) {
        return createWarnings(warningsMode, source, "warnings during evaluation of [{}]");
    }

    private static Warnings createWarnings(DriverContext.WarningsMode warningsMode, WarningSourceLocation source, String first) {
        switch (warningsMode) {
            case COLLECT -> {
                return new Warnings(source.lineNumber(), source.columnNumber(), source.viewName(), source.text(), first);
            }
            case IGNORE -> {
                return NOOP_WARNINGS;
            }
        }
        throw new IllegalStateException("Unreachable");
    }

    private static Warnings createWarnings(
        DriverContext.WarningsMode warningsMode,
        int lineNumber,
        int columnNumber,
        String viewName,
        String sourceText,
        String first
    ) {
        switch (warningsMode) {
            case COLLECT -> {
                return new Warnings(lineNumber, columnNumber, viewName, sourceText, first);
            }
            case IGNORE -> {
                return NOOP_WARNINGS;
            }
        }
        throw new IllegalStateException("Unreachable");
    }

    private final String location;
    private final String firstExceptionWarning;
    private final String nonExceptionWarningPrefix;
    private final Set<String> emittedNonExceptionWarnings = new HashSet<>();

    private int addedWarnings;
    private boolean exceptionWarningEmitted = false;

    private Warnings(int lineNumber, int columnNumber, String viewName, String sourceText, String firstExceptionWarning) {
        if (viewName == null) {
            this.location = format("Line {}:{}: ", lineNumber, columnNumber);
            this.nonExceptionWarningPrefix = format("Line {}:{} [{}]: ", lineNumber, columnNumber, sourceText);
        } else {
            this.location = format("Line {}:{} (in view [{}]): ", lineNumber, columnNumber, viewName);
            this.nonExceptionWarningPrefix = format("Line {}:{} [{}] (in view [{}]): ", lineNumber, columnNumber, sourceText, viewName);
        }
        this.firstExceptionWarning = format(
            null,
            "{}" + firstExceptionWarning + ". Only first {} failures recorded.",
            location,
            sourceText,
            MAX_ADDED_WARNINGS
        );
    }

    public void registerException(Exception exception) {
        registerException(exception.getClass(), exception.getMessage());
    }

    /**
     * Register an exception to be included in the warnings.
     * <p>
     *     This overload avoids the need to instantiate the exception, which can be expensive.
     *     Instead, it asks only the required pieces to build the warning.
     * </p>
     */
    public void registerException(Class<? extends Exception> exceptionClass, String message) {
        if (addedWarnings < MAX_ADDED_WARNINGS) {
            if (exceptionWarningEmitted == false) {
                exceptionWarningEmitted = true;
                addWarning(firstExceptionWarning);
            }
            // location needs to be added to the exception too, since the headers are deduplicated
            addWarning(location + exceptionClass.getName() + ": " + message);
            addedWarnings++;
        }
    }

    /**
     * Register a custom warning message (not tied to an exception).
     * Even if the very same warning is registered multiple times, it will only be emitted once.
     * This method therefore caches the emitted message and should not be called with non-constant messages!
     */
    public void registerWarning(String message) {
        if (addedWarnings < MAX_ADDED_WARNINGS && !emittedNonExceptionWarnings.contains(message)) {
            emittedNonExceptionWarnings.add(message);
            addWarning(nonExceptionWarningPrefix + message);
            addedWarnings++;
        }
    }
}

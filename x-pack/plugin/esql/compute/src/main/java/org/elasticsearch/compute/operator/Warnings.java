/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import java.util.HashSet;
import java.util.Set;

import static org.elasticsearch.common.logging.LoggerMessageFormat.format;

/**
 * Utilities to collect warnings for running an executor.
 */
public class Warnings {
    static final int MAX_ADDED_WARNINGS = 20;

    private static final String TREATED_AS_FALSE = "evaluation of [{}] failed, treating result as false";

    public static final Warnings NOOP_WARNINGS = new Warnings(null, -1, -2, null, "", "") {
        @Override
        public void registerException(Exception exception) {
            // this space intentionally left blank
        }

        @Override
        public void registerException(Class<? extends Exception> exceptionClass, String message) {
            // this space intentionally left blank
        }

        @Override
        public void registerWarning(String message) {
            // this space intentionally left blank
        }
    };

    /**
     * Create a new warnings object that writes into the given context's per-driver sink.
     * @param driverContext The context owning the per-driver warnings sink and the {@link DriverContext#warningsMode()}
     * @param source The source location information for warnings
     * @return A warnings collector object
     */
    public static Warnings createWarnings(DriverContext driverContext, WarningSourceLocation source) {
        return createWarnings(driverContext, source, "evaluation of [{}] failed, treating result as null");
    }

    /**
     * Create a new warnings object that writes into the given context's per-driver sink and warns that
     * it treats the result as {@code false}.
     * @param driverContext The context owning the per-driver warnings sink and the {@link DriverContext#warningsMode()}
     * @param source The source location information for warnings
     * @return A warnings collector object
     */
    public static Warnings createWarningsTreatedAsFalse(DriverContext driverContext, WarningSourceLocation source) {
        return createWarnings(driverContext, source, TREATED_AS_FALSE);
    }

    /**
     * The header announcing that failures at {@code source} are treated as {@code false}, as
     * {@link #createWarningsTreatedAsFalse} would emit before the first {@link #registerException}.
     * <p>
     *     This and {@link #exceptionWarning} exist for callers that have to raise these warnings
     *     without a {@link DriverContext} to collect them, so that the text they produce stays
     *     identical to the evaluator's.
     * </p>
     */
    public static String firstTreatedAsFalseWarning(WarningSourceLocation source) {
        return firstExceptionWarning(locationPrefix(source), source.text(), TREATED_AS_FALSE);
    }

    /**
     * The warning reporting one failure at {@code source}, as {@link #registerException} would emit it.
     */
    public static String exceptionWarning(WarningSourceLocation source, Class<? extends Exception> exceptionClass, String message) {
        return locationPrefix(source) + exceptionClass.getName() + ": " + message;
    }

    private static String locationPrefix(WarningSourceLocation source) {
        return locationPrefix(source.lineNumber(), source.columnNumber(), source.viewName());
    }

    /**
     * Every warning has to carry this, since the headers are deduplicated.
     */
    private static String locationPrefix(int lineNumber, int columnNumber, String viewName) {
        return viewName == null
            ? format("Line {}:{}: ", lineNumber, columnNumber)
            : format("Line {}:{} (in view [{}]): ", lineNumber, columnNumber, viewName);
    }

    private static String firstExceptionWarning(String location, String sourceText, String first) {
        return format(null, "{}" + first + ". Only first {} failures recorded.", location, sourceText, MAX_ADDED_WARNINGS);
    }

    /**
     * Create a new warnings object that writes into the given context's per-driver sink and warns that
     * evaluation resulted in warnings.
     * @param driverContext The context owning the per-driver warnings sink and the {@link DriverContext#warningsMode()}
     * @param source The source location information for warnings
     * @return A warnings collector object
     */
    public static Warnings createOnlyWarnings(DriverContext driverContext, WarningSourceLocation source) {
        return createWarnings(driverContext, source, "warnings during evaluation of [{}]");
    }

    private static Warnings createWarnings(DriverContext driverContext, WarningSourceLocation source, String first) {
        switch (driverContext.warningsMode()) {
            case COLLECT -> {
                return new Warnings(driverContext, source.lineNumber(), source.columnNumber(), source.viewName(), source.text(), first);
            }
            case IGNORE -> {
                return NOOP_WARNINGS;
            }
        }
        throw new IllegalStateException("Unreachable");
    }

    private final DriverContext driverContext;
    private final String location;
    private final String firstExceptionWarning;
    private final String nonExceptionWarningPrefix;
    private final Set<String> emittedNonExceptionWarnings = new HashSet<>();

    private int addedWarnings;
    private boolean exceptionWarningEmitted = false;

    private Warnings(
        DriverContext driverContext,
        int lineNumber,
        int columnNumber,
        String viewName,
        String sourceText,
        String firstExceptionWarning
    ) {
        this.driverContext = driverContext;
        this.location = locationPrefix(lineNumber, columnNumber, viewName);
        if (viewName == null) {
            this.nonExceptionWarningPrefix = format("Line {}:{} [{}]: ", lineNumber, columnNumber, sourceText);
        } else {
            this.nonExceptionWarningPrefix = format("Line {}:{} [{}] (in view [{}]): ", lineNumber, columnNumber, sourceText, viewName);
        }
        this.firstExceptionWarning = firstExceptionWarning(location, sourceText, firstExceptionWarning);
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
                driverContext.addWarning(firstExceptionWarning);
            }
            // location needs to be added to the exception too, since the headers are deduplicated
            driverContext.addWarning(location + exceptionClass.getName() + ": " + message);
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
            driverContext.addWarning(nonExceptionWarningPrefix + message);
            addedWarnings++;
        }
    }
}

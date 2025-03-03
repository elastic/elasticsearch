/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import static org.elasticsearch.common.logging.HeaderWarning.addWarning;
import static org.elasticsearch.common.logging.LoggerMessageFormat.format;

/**
 * Utilities to collect warnings for running an executor.
 */
public class Warnings {
    static final int MAX_ADDED_WARNINGS = 20;

    public static final Warnings NOOP_WARNINGS = new Warnings(-1, -2, "", "") {
        @Override
        public void registerException(Exception exception) {
            // this space intentionally left blank
        }
    };

    /**
     * Create a new warnings object based on the given mode
     * @param warningsMode The warnings collection strategy to use
     * @param lineNumber The line number of the source text. Same as `source.getLineNumber()`
     * @param columnNumber The column number of the source text. Same as `source.getColumnNumber()`
     * @param sourceText The source text that caused the warning. Same as `source.text()`
     * @return A warnings collector object
     */
    // TODO: rename to createWarningsTreatedAsNull
    public static Warnings createWarnings(DriverContext.WarningsMode warningsMode, int lineNumber, int columnNumber, String sourceText) {
        return createWarnings(warningsMode, lineNumber, columnNumber, sourceText, "evaluation of [{}] failed, treating result as null");
    }

    /**
     * Create a new warnings object based on the given mode which warns that
     * it treats the result as {@code false}.
     * @param warningsMode The warnings collection strategy to use
     * @param lineNumber The line number of the source text. Same as `source.getLineNumber()`
     * @param columnNumber The column number of the source text. Same as `source.getColumnNumber()`
     * @param sourceText The source text that caused the warning. Same as `source.text()`
     * @return A warnings collector object
     */
    public static Warnings createWarningsTreatedAsFalse(
        DriverContext.WarningsMode warningsMode,
        int lineNumber,
        int columnNumber,
        String sourceText
    ) {
        return createWarnings(warningsMode, lineNumber, columnNumber, sourceText, "evaluation of [{}] failed, treating result as false");
    }

    /**
     * Create a new warnings object based on the given mode which warns that
     * evaluation resulted in warnings.
     * @param warningsMode The warnings collection strategy to use
     * @param lineNumber The line number of the source text. Same as `source.getLineNumber()`
     * @param columnNumber The column number of the source text. Same as `source.getColumnNumber()`
     * @param sourceText The source text that caused the warning. Same as `source.text()`
     * @return A warnings collector object
     */
    // TODO: rename to createWarnings
    public static Warnings createOnlyWarnings(
        DriverContext.WarningsMode warningsMode,
        int lineNumber,
        int columnNumber,
        String sourceText
    ) {
        return createWarnings(warningsMode, lineNumber, columnNumber, sourceText, "warnings during evaluation of [{}]");
    }

    private static Warnings createWarnings(
        DriverContext.WarningsMode warningsMode,
        int lineNumber,
        int columnNumber,
        String sourceText,
        String first
    ) {
        switch (warningsMode) {
            case COLLECT -> {
                return new Warnings(lineNumber, columnNumber, sourceText, first);
            }
            case IGNORE -> {
                return NOOP_WARNINGS;
            }
        }
        throw new IllegalStateException("Unreachable");
    }

    private final String location;
    private final String first;

    private int addedWarnings;

    private Warnings(int lineNumber, int columnNumber, String sourceText, String first) {
        this.location = format("Line {}:{}: ", lineNumber, columnNumber);
        this.first = format(null, "{}" + first + ". Only first {} failures recorded.", location, sourceText, MAX_ADDED_WARNINGS);
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
            if (addedWarnings == 0) {
                addWarning(first);
            }
            // location needs to be added to the exception too, since the headers are deduplicated
            addWarning(location + exceptionClass.getName() + ": " + message);
            addedWarnings++;
        }
    }
}

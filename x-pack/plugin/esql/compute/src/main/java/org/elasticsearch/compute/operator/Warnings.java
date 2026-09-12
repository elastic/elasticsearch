/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import static org.elasticsearch.common.logging.LoggerMessageFormat.format;

/**
 * Utilities to collect warnings for running an executor.
 */
public class Warnings {
    static final int MAX_ADDED_WARNINGS = 20;

    public static final Warnings NOOP_WARNINGS = new Warnings(null, -1, -2, null, "", "") {
        @Override
        public void registerException(Exception exception) {}

        @Override
        public void registerException(Class<? extends Exception> exceptionClass, String message) {}
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
        return createWarnings(driverContext, source, "evaluation of [{}] failed, treating result as false");
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

    /**
     * Create a new warnings object that writes into the given context's per-driver sink.
     * @param driverContext The context owning the per-driver warnings sink and the {@link DriverContext#warningsMode()}
     * @param lineNumber The line number of the source text. Same as {@code source.getLineNumber()}
     * @param columnNumber The column number of the source text. Same as {@code source.getColumnNumber()}
     * @param sourceText The source text that caused the warning. Same as {@code source.text()}
     * @return A warnings collector object
     */
    public static Warnings createWarnings(DriverContext driverContext, int lineNumber, int columnNumber, String sourceText) {
        return createWarnings(
            driverContext,
            lineNumber,
            columnNumber,
            null,
            sourceText,
            "evaluation of [{}] failed, treating result as null"
        );
    }

    /**
     * Create a new warnings object that writes into the given context's per-driver sink and warns that
     * it treats the result as {@code false}.
     * @param driverContext The context owning the per-driver warnings sink and the {@link DriverContext#warningsMode()}
     * @param lineNumber The line number of the source text. Same as {@code source.getLineNumber()}
     * @param columnNumber The column number of the source text. Same as {@code source.getColumnNumber()}
     * @param sourceText The source text that caused the warning. Same as {@code source.text()}
     * @return A warnings collector object
     */
    public static Warnings createWarningsTreatedAsFalse(DriverContext driverContext, int lineNumber, int columnNumber, String sourceText) {
        return createWarnings(
            driverContext,
            lineNumber,
            columnNumber,
            null,
            sourceText,
            "evaluation of [{}] failed, treating result as false"
        );
    }

    /**
     * Create a new warnings object that writes into the given context's per-driver sink and warns that
     * evaluation resulted in warnings.
     * @param driverContext The context owning the per-driver warnings sink and the {@link DriverContext#warningsMode()}
     * @param lineNumber The line number of the source text. Same as {@code source.getLineNumber()}
     * @param columnNumber The column number of the source text. Same as {@code source.getColumnNumber()}
     * @param sourceText The source text that caused the warning. Same as {@code source.text()}
     * @return A warnings collector object
     */
    public static Warnings createOnlyWarnings(DriverContext driverContext, int lineNumber, int columnNumber, String sourceText) {
        return createWarnings(driverContext, lineNumber, columnNumber, null, sourceText, "warnings during evaluation of [{}]");
    }

    private static Warnings createWarnings(DriverContext driverContext, WarningSourceLocation source, String first) {
        switch (driverContext.warningsMode()) {
            case COLLECT -> {
                return new Warnings(driverContext, source.lineNumber(), source.columnNumber(), null, source.text(), first);
            }
            case IGNORE -> {
                return NOOP_WARNINGS;
            }
        }
        throw new IllegalStateException("Unreachable");
    }

    private static Warnings createWarnings(
        DriverContext driverContext,
        int lineNumber,
        int columnNumber,
        String viewName,
        String sourceText,
        String first
    ) {
        switch (driverContext.warningsMode()) {
            case COLLECT -> {
                return new Warnings(driverContext, lineNumber, columnNumber, viewName, sourceText, first);
            }
            case IGNORE -> {
                return NOOP_WARNINGS;
            }
        }
        throw new IllegalStateException("Unreachable");
    }

    private final DriverContext driverContext;
    private final String location;
    private final String first;

    private int addedWarnings;

    private Warnings(DriverContext driverContext, int lineNumber, int columnNumber, String viewName, String sourceText, String first) {
        this.driverContext = driverContext;
        if (viewName == null) {
            this.location = format("Line {}:{}: ", lineNumber, columnNumber);
        } else {
            this.location = format("Line {}:{} (in view [{}]): ", lineNumber, columnNumber, viewName);
        }
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
                driverContext.addWarning(first);
            }
            // location needs to be added to the exception too, since the headers are deduplicated
            driverContext.addWarning(location + exceptionClass.getName() + ": " + message);
            addedWarnings++;
        }
    }
}

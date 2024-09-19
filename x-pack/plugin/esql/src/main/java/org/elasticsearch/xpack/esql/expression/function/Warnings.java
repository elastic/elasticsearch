/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function;

import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.xpack.esql.core.tree.Source;

import static org.elasticsearch.common.logging.HeaderWarning.addWarning;
import static org.elasticsearch.common.logging.LoggerMessageFormat.format;

/**
 * Utilities to collect warnings for running an executor.
 */
public class Warnings {
    static final int MAX_ADDED_WARNINGS = 20;

    private final String location;
    private final String first;

    private int addedWarnings;

    public static final Warnings NOOP_WARNINGS = new Warnings(Source.EMPTY) {
        @Override
        public void registerException(Exception exception) {
            // this space intentionally left blank
        }
    };

    /**
     * Create a new warnings object based on the given mode which warns that
     * it treats the result as {@code null}.
     * @param warningsMode The warnings collection strategy to use
     * @param source used to indicate where in the query the warning occurred
     * @return A warnings collector object
     */
    public static Warnings createWarnings(DriverContext.WarningsMode warningsMode, Source source) {
        return createWarnings(warningsMode, source, "treating result as null");
    }

    /**
     * Create a new warnings object based on the given mode which warns that
     * it treats the result as {@code false}.
     * @param warningsMode The warnings collection strategy to use
     * @param source used to indicate where in the query the warning occurred
     * @return A warnings collector object
     */
    public static Warnings createWarningsTreatedAsFalse(DriverContext.WarningsMode warningsMode, Source source) {
        return createWarnings(warningsMode, source, "treating result as false");
    }

    /**
     * Create a new warnings object based on the given mode
     * @param warningsMode The warnings collection strategy to use
     * @param source used to indicate where in the query the warning occurred
     * @param first warning message attached to the first result
     * @return A warnings collector object
     */
    public static Warnings createWarnings(DriverContext.WarningsMode warningsMode, Source source, String first) {
        return switch (warningsMode) {
            case COLLECT -> new Warnings(source, first);
            case IGNORE -> NOOP_WARNINGS;
        };
    }

    public Warnings(Source source) {
        this(source, "treating result as null");
    }

    public Warnings(Source source, String first) {
        location = format("Line {}:{}: ", source.source().getLineNumber(), source.source().getColumnNumber());
        this.first = format(
            null,
            "{}evaluation of [{}] failed, {}. Only first {} failures recorded.",
            location,
            source.text(),
            first,
            MAX_ADDED_WARNINGS
        );
    }

    public void registerException(Exception exception) {
        if (addedWarnings < MAX_ADDED_WARNINGS) {
            if (addedWarnings == 0) {
                addWarning(first);
            }
            // location needs to be added to the exception too, since the headers are deduplicated
            addWarning(location + exception.getClass().getName() + ": " + exception.getMessage());
            addedWarnings++;
        }
    }
}

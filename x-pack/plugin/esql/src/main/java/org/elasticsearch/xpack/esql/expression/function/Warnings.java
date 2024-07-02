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
     * Create a new warnings object based on the given mode
     * @param warningsMode The warnings collection strategy to use
     * @param source used to indicate where in the query the warning occured
     * @return A warnings collector object
     */
    public static Warnings createWarnings(DriverContext.WarningsMode warningsMode, Source source) {
        switch (warningsMode) {
            case COLLECT -> {
                return new Warnings(source);
            }
            case IGNORE -> {
                return NOOP_WARNINGS;
            }
        }
        throw new IllegalStateException("Unreachable");
    }

    public Warnings(Source source) {
        location = format("Line {}:{}: ", source.source().getLineNumber(), source.source().getColumnNumber());
        first = format(
            null,
            "{}evaluation of [{}] failed, treating result as null. Only first {} failures recorded.",
            location,
            source.text(),
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

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function;

import org.elasticsearch.xpack.ql.tree.Source;

import static org.elasticsearch.common.logging.HeaderWarning.addWarning;

/**
 * Utilities to collect warnings for running an executor.
 */
public class Warnings {
    static final int MAX_ADDED_WARNINGS = 20;

    private final Source source;

    private int addedWarnings;

    public Warnings(Source source) {
        this.source = source;
    }

    public void registerException(Exception exception) {
        if (addedWarnings < MAX_ADDED_WARNINGS) {
            if (addedWarnings == 0) {
                addWarning(
                    "Line {}:{}: evaluation of [{}] failed, treating result as null. Only first {} failures recorded.",
                    source.source().getLineNumber(),
                    source.source().getColumnNumber(),
                    source.text(),
                    MAX_ADDED_WARNINGS
                );
            }
            addWarning(exception.getClass().getName() + ": " + exception.getMessage());
            addedWarnings++;
        }
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.test;

import org.elasticsearch.compute.operator.WarningSourceLocation;

/**
 * A {@link WarningSourceLocation} for tests. Supports configurable text, view name, and line/column.
 */
public record TestWarningsSource(String text, String viewName, int lineNumber, int columnNumber) implements WarningSourceLocation {
    public static final TestWarningsSource INSTANCE = new TestWarningsSource("source", null, 1, 1);

    public TestWarningsSource(String text) {
        this(text, null, 1, 1);
    }

    public TestWarningsSource(String text, String viewName) {
        this(text, viewName, 1, 1);
    }
}

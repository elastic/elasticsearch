/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

/**
 * Interface for providing source location information for warnings.
 * This is implemented by Source in the esql module to provide location
 * information without creating a compile-time dependency from compute to esql-core.
 */
public interface WarningSourceLocation {
    /**
     * The line number (1-indexed) of the source text.
     */
    int lineNumber();

    /**
     * The column number (1-indexed) of the source text.
     */
    int columnNumber();

    /**
     * The name of the view this source came from, or null if from the original query.
     */
    String viewName();

    /**
     * The source text that caused the warning.
     */
    String text();
}

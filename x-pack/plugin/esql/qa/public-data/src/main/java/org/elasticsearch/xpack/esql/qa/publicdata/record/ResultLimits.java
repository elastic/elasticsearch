/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.publicdata.record;

/**
 * Per-query result-set caps. These bound each <em>result set</em>, not the containing csv-spec
 * file: a corpus's whole workload lives in one file, so the file has no line cap, but no single
 * expected table may exceed these — expected tables are human-reviewed, and review quality dies at
 * scale.
 */
public final class ResultLimits {

    /** Default cap on expected-table rows; exceeding it needs an explicit reviewed reason. */
    public static final int DEFAULT_MAX_ROWS = 300;
    /** Absolute cap; no expected table may exceed this. */
    public static final int ABSOLUTE_MAX_ROWS = 1000;

    private ResultLimits() {}

    /**
     * Enforces a test's declared {@code // max-rows:} limit against what the query actually
     * returned. A violation is a suite-authoring bug (the query lacks a LIMIT or its LIMIT
     * disagrees with the declaration), not a product bug — fail loudly either way.
     */
    public static void enforce(String testName, int declaredMaxRows, int actualRows) {
        if (declaredMaxRows > ABSOLUTE_MAX_ROWS) {
            throw new AssertionError(
                "test [" + testName + "] declares max-rows " + declaredMaxRows + " above the absolute cap " + ABSOLUTE_MAX_ROWS
            );
        }
        if (actualRows > declaredMaxRows) {
            throw new AssertionError(
                "test [" + testName + "] returned " + actualRows + " rows, above its declared max-rows " + declaredMaxRows
            );
        }
    }
}

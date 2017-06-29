/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc.compare;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import java.nio.file.Path;

/**
 * Tests for basic {@code SELECT} statements without {@code WHERE} or {@code GROUP BY} or functions.
 */
public class SelectIT extends CompareToH2BaseTestCase {
    public SelectIT(String queryName, String query, Integer lineNumber, Path source) {
        super(queryName, query, lineNumber, source);
    }

    @ParametersFactory
    public static Iterable<Object[]> queries() throws Exception {
        return readScriptSpec("select");
    }
}

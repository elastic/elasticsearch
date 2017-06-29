/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc.query;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import java.nio.file.Path;

public class SelectIT extends CompareToH2BaseTestCase {
    public SelectIT(String queryName, String query, Integer lineNumber, Path source) {
        super(queryName, query, lineNumber, source);
    }

    @ParametersFactory
    public static Iterable<Object[]> queries() throws Exception {
        return readScriptSpec("select");
    }
}

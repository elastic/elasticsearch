/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc.integration.query.function.aggregate;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.xpack.sql.jdbc.integration.query.CompareToH2BaseTestCase;

import java.nio.file.Path;

public class AggSpecTests extends CompareToH2BaseTestCase {
    public AggSpecTests(String queryName, String query, Integer lineNumber, Path source) {
        super(queryName, query, lineNumber, source);
    }

    @ParametersFactory
    public static Iterable<Object[]> queries() throws Exception {
        return readScriptSpec("/org/elasticsearch/sql/jdbc/integration/query/function/aggregate/agg.spec");
    }
}

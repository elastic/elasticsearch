/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.generator;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.generator.function.CompositeFunctionGenerator;

import java.util.List;

public class CompositeFunctionGeneratorTests extends ESTestCase {

    public void testCompositeExpressionWithMatchingColumn() {
        List<Column> columns = List.of(new Column("x", "integer", List.of("integer")));
        // With an integer column and depth >= 1, a result is expected (either a plain field or a function call)
        String expr = CompositeFunctionGenerator.compositeExpression(columns, false, "integer", 3);
        assertNotNull("expected a non-null expression for an integer column", expr);
        assertFalse("expected a non-empty expression", expr.isBlank());
    }

    public void testCompositeExpressionDatetimeEquivalence() {
        // @timestamp is typed as "datetime" in the generator schema, but Kibana JSONs use "date".
        // The equivalence fix must allow composite expressions targeting "datetime" to be generated.
        List<Column> columns = List.of(new Column("@timestamp", "datetime", List.of("datetime")));
        String expr = CompositeFunctionGenerator.compositeExpression(columns, false, "datetime", 3);
        assertNotNull("expected a non-null expression for a datetime column", expr);
    }

    public void testCompositeExpressionDepthZeroReturnsLeafOrNull() {
        List<Column> columns = List.of(new Column("k", "keyword", List.of("keyword")));
        for (int i = 0; i < 20; i++) {
            String expr = CompositeFunctionGenerator.compositeExpression(columns, false, "keyword", 0);
            // At depth 0, only a leaf is possible — either the column name or null
            if (expr != null) {
                assertFalse("leaf expression must not contain parentheses", expr.contains("("));
            }
        }
    }
}

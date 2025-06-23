/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.predicate.operator;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.DocsV3Support;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.junit.AfterClass;

import java.util.List;
import java.util.Map;

public class CastOperatorTests extends ESTestCase {
    public void testDummy() {
        assert true;
    }

    @AfterClass
    public static void renderDocs() throws Exception {
        if (System.getProperty("generateDocs") == null) {
            return;
        }
        DocsV3Support.OperatorConfig op = new DocsV3Support.OperatorConfig(
            "cast",
            "::",
            TestCastOperator.class,
            DocsV3Support.OperatorCategory.CAST
        );
        var docs = new DocsV3Support.OperatorsDocsSupport("cast", CastOperatorTests.class, op, CastOperatorTests::signatures);
        docs.renderSignature();
        docs.renderDocs();
    }

    public static Map<List<DataType>, DataType> signatures() {
        // The cast operator cannot produce sensible signatures unless we consider the type as an extra parameter
        return Map.of();
    }

    /**
     * This class only exists to provide FunctionInfo for the documentation
     */
    public class TestCastOperator {
        @FunctionInfo(
            operator = "::",
            returnType = {},
            description = "The `::` operator provides a convenient alternative syntax to the TO_<type> "
                + "[conversion functions](/reference/query-languages/esql/functions-operators/type-conversion-functions.md).",
            examples = { @Example(file = "convert", tag = "docsCastOperator") }
        )
        public TestCastOperator(
            @Param(
                name = "field",
                type = {
                    "boolean",
                    "cartesian_point",
                    "cartesian_shape",
                    "date",
                    "date_nanos",
                    "double",
                    "geo_point",
                    "geo_shape",
                    "integer",
                    "ip",
                    "keyword",
                    "long",
                    "text",
                    "unsigned_long",
                    "version" },
                description = "Input value. The input can be a single- or multi-valued column or an expression."
            ) Expression v
        ) {}
    }
}

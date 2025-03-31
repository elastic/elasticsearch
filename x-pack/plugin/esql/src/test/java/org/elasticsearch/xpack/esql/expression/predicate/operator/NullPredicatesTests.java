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
import org.elasticsearch.xpack.esql.expression.function.AbstractFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.DocsV3Support;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToStringTests;
import org.junit.AfterClass;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * In the documentation we document `IS NULL` and `IS NOT NULL` together.
 */
public class NullPredicatesTests extends ESTestCase {
    public void testDummy() {
        assert true;
    }

    @AfterClass
    public static void renderDocs() throws IOException {
        if (System.getProperty("generateDocs") == null) {
            return;
        }
        DocsV3Support.OperatorConfig op = new DocsV3Support.OperatorConfig(
            "predicates",
            "IS NULL and IS NOT NULL",
            TestCastOperator.class,
            DocsV3Support.OperatorCategory.UNARY,
            false
        );
        var docs = new DocsV3Support.OperatorsDocsSupport("predicates", NullPredicatesTests.class, op, NullPredicatesTests::signatures);
        docs.renderSignature();
        docs.renderDocs();
    }

    public static Map<List<DataType>, DataType> signatures() {
        // TODO: Verify the correct datatypes for this
        Map<List<DataType>, DataType> toString = AbstractFunctionTestCase.signatures(ToStringTests.class);
        Map<List<DataType>, DataType> results = new LinkedHashMap<>();
        for (var entry : toString.entrySet()) {
            DataType dataType = entry.getKey().getFirst();
            results.put(List.of(dataType), DataType.BOOLEAN);
        }
        return results;
    }

    /**
     * This class only exists to provide FunctionInfo for the documentation
     */
    public class TestCastOperator {
        @FunctionInfo(
            operator = "predicates",
            returnType = {},
            description = "For NULL comparison use the `IS NULL` and `IS NOT NULL` predicates:",
            examples = { @Example(file = "null", tag = "is-null"), @Example(file = "null", tag = "is-not-null") }
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

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
    public static void renderDocs() throws Exception {
        if (System.getProperty("generateDocs") == null) {
            return;
        }
        renderNullPredicate(
            new DocsV3Support.OperatorConfig(
                "predicates",
                "IS NULL and IS NOT NULL",
                TestNullPredicates.class,
                DocsV3Support.OperatorCategory.UNARY
            )
        );
        renderNullPredicate(
            new DocsV3Support.OperatorConfig(
                "is_null",
                "IS NULL",
                TestIsNullPredicate.class,
                DocsV3Support.OperatorCategory.NULL_PREDICATES
            )
        );
        renderNullPredicate(
            new DocsV3Support.OperatorConfig(
                "is_not_null",
                "IS NOT NULL",
                TestIsNotNullPredicate.class,
                DocsV3Support.OperatorCategory.NULL_PREDICATES
            )
        );
    }

    private static void renderNullPredicate(DocsV3Support.OperatorConfig op) throws Exception {
        var docs = new DocsV3Support.OperatorsDocsSupport(op.name(), NullPredicatesTests.class, op, NullPredicatesTests::signatures);
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
    public class TestNullPredicates {
        @FunctionInfo(
            returnType = {},
            description = "For NULL comparison use the `IS NULL` and `IS NOT NULL` predicates.",
            examples = { @Example(file = "null", tag = "is-null"), @Example(file = "null", tag = "is-not-null") }
        )
        public TestNullPredicates(
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

    /**
     * This class only exists to provide FunctionInfo for the documentation
     */
    public class TestIsNullPredicate {
        @FunctionInfo(
            operator = "IS NULL",
            returnType = {},
            description = "Use `IS NULL` to filter data based on whether the field exists or not.",
            examples = { @Example(file = "null", tag = "is-null") }
        )
        public TestIsNullPredicate(
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

    /**
     * This class only exists to provide FunctionInfo for the documentation
     */
    public class TestIsNotNullPredicate {
        @FunctionInfo(
            operator = "IS NOT NULL",
            returnType = {},
            description = "Use `IS NOT NULL` to filter data based on whether the field exists or not.",
            examples = { @Example(file = "null", tag = "is-not-null") }
        )
        public TestIsNotNullPredicate(
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

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.grouping;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;

/**
 * Dummy test implementation for Categorize. Used just to generate documentation.
 * <p>
 *     Most test cases are currently skipped as this function can't build an evaluator.
 * </p>
 */
public class CategorizeTests extends AbstractScalarFunctionTestCase {
    public CategorizeTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();
        for (DataType dataType : List.of(DataType.KEYWORD, DataType.TEXT)) {
            suppliers.add(
                new TestCaseSupplier(
                    "text with " + dataType.typeName(),
                    List.of(dataType),
                    () -> new TestCaseSupplier.TestCase(
                        List.of(new TestCaseSupplier.TypedData(new BytesRef(""), dataType, "field")),
                        "",
                        DataType.KEYWORD,
                        equalTo(new BytesRef(""))
                    ).withoutEvaluator()
                )
            );
        }
        return parameterSuppliersFromTypedDataWithDefaultChecks(true, suppliers, (v, p) -> "string");
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new Categorize(source, args.get(0));
    }
}

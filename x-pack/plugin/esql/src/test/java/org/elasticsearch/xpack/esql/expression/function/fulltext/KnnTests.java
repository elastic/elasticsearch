/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.fulltext;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MapExpression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.elasticsearch.xpack.esql.expression.function.vector.Knn;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.esql.core.type.DataType.BOOLEAN;
import static org.elasticsearch.xpack.esql.core.type.DataType.DENSE_VECTOR;
import static org.elasticsearch.xpack.esql.core.type.DataType.TEXT;
import static org.hamcrest.Matchers.equalTo;

public class KnnTests extends AbstractFullTextFunctionTestCase {

    public KnnTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        return parameterSuppliersFromTypedData(addNullFieldTestCases(addFunctionNamedParams(testCaseSuppliers(), mapExpressionSupplier())));
    }

    private static List<TestCaseSupplier> testCaseSuppliers() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();

        suppliers.add(
            new TestCaseSupplier(
                List.of(DENSE_VECTOR, DENSE_VECTOR),
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(randomDenseVector(), DENSE_VECTOR, "dense_vector field"),
                        new TestCaseSupplier.TypedData(randomDenseVector(), DENSE_VECTOR, "query")
                    ),
                    equalTo("KnnEvaluator" + KnnTests.class.getSimpleName()),
                    BOOLEAN,
                    equalTo(true)
                )
            )
        );
        suppliers.add(
            new TestCaseSupplier(
                List.of(TEXT, DENSE_VECTOR),
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(
                            new TestCaseSupplier.TypedData(randomAlphaOfLength(10), TEXT, "text field"),
                            TEXT,
                            "text field"
                        ),
                        new TestCaseSupplier.TypedData(randomDenseVector(), DENSE_VECTOR, "query")
                    ),
                    equalTo("KnnEvaluator" + KnnTests.class.getSimpleName()),
                    BOOLEAN,
                    equalTo(true)
                )
            )
        );

        return suppliers;
    }

    private static List<Float> randomDenseVector() {
        int dimensions = randomIntBetween(64, 128);
        List<Float> vector = new ArrayList<>();
        for (int i = 0; i < dimensions; i++) {
            vector.add(randomFloat());
        }
        return vector;
    }

    private static Supplier<MapExpression> mapExpressionSupplier() {
        return () -> new MapExpression(Source.EMPTY, List.of(Literal.keyword(Source.EMPTY, randomAlphaOfLength(10))));
    }

    @Override
    public void testLiteralExpressions() {
        // Knn test data contains values that cannot be serialized when wrapped in a Literal,
        // so we skip the serialization path and only check type resolution.
        assumeTrue("Data can't be converted to literals", testCase.canGetDataAsLiterals());
        Expression expression = build(testCase.getSource(), testCase.getDataAsLiterals());
        assertFalse("expected resolved", expression.typeResolved().unresolved());
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return build(new Knn(source, args.get(0), args.get(1), args.size() > 2 ? args.get(2) : null), args);
    }
}

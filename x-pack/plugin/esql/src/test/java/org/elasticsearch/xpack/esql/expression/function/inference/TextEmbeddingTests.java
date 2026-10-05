/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.inference;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MapExpression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.AbstractFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.FunctionName;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.hamcrest.Matchers;

import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.esql.core.type.DataType.DENSE_VECTOR;
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.elasticsearch.xpack.esql.core.type.DataType.UNSUPPORTED;
import static org.hamcrest.Matchers.equalTo;

/**
 * Extends {@link AbstractFunctionTestCase} directly rather than {@link AbstractScalarFunctionTestCase}:
 * {@link TextEmbedding} has no per-row evaluator and is never executed as an ordinary scalar function. It can only be
 * folded, via a dedicated pre-optimizer pass ({@code FoldInferenceFunctions}) that runs a real inference call outside
 * {@link org.elasticsearch.xpack.esql.core.expression.Expression#fold}, so the evaluator-based test machinery in
 * {@link AbstractScalarFunctionTestCase} does not apply here. That folding behavior is already covered end-to-end in
 * {@code InferenceFunctionEvaluatorTests}; this class is limited to exercising {@link TextEmbedding#resolveType}.
 */
@FunctionName("text_embedding")
public class TextEmbeddingTests extends AbstractFunctionTestCase {
    public TextEmbeddingTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        return parameterSuppliersFromTypedData(
            List.of(
                new TestCaseSupplier(
                    List.of(KEYWORD, KEYWORD, UNSUPPORTED),
                    () -> new TestCaseSupplier.TestCase(
                        List.of(
                            new TestCaseSupplier.TypedData(randomBytesReference(10).toBytesRef(), KEYWORD, "text"),
                            new TestCaseSupplier.TypedData(randomBytesReference(10).toBytesRef(), KEYWORD, "inference_id"),
                            new TestCaseSupplier.TypedData(
                                new MapExpression(
                                    Source.EMPTY,
                                    List.of(Literal.keyword(Source.EMPTY, "timeout"), Literal.keyword(Source.EMPTY, "30s"))
                                ),
                                UNSUPPORTED,
                                "options"
                            ).forceLiteral()
                        ),
                        Matchers.blankOrNullString(),
                        DENSE_VECTOR,
                        equalTo(true)
                    )
                ),
                new TestCaseSupplier(
                    List.of(KEYWORD, KEYWORD),
                    () -> new TestCaseSupplier.TestCase(
                        List.of(
                            new TestCaseSupplier.TypedData(randomBytesReference(10).toBytesRef(), KEYWORD, "text"),
                            new TestCaseSupplier.TypedData(randomBytesReference(10).toBytesRef(), KEYWORD, "inference_id")
                        ),
                        Matchers.blankOrNullString(),
                        DENSE_VECTOR,
                        equalTo(true)
                    )
                )
            )
        );
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new TextEmbedding(source, args.get(0), args.get(1), args.size() > 2 ? args.get(2) : null);
    }

    @Override
    protected boolean canSerialize() {
        return false;
    }
}

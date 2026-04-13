/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.inference;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MapExpression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.AbstractFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.FunctionName;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.hamcrest.Matchers;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.esql.core.type.DataType.DENSE_VECTOR;
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.elasticsearch.xpack.esql.core.type.DataType.UNSUPPORTED;
import static org.hamcrest.Matchers.equalTo;

@FunctionName("embedding")
public class EmbeddingTests extends AbstractFunctionTestCase {

    @Before
    public void checkCapability() {
        assumeTrue("Embedding function must be enabled", EsqlCapabilities.Cap.EMBEDDING_FUNCTION.isEnabled());
    }

    public EmbeddingTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();

        suppliers.add(
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
            )
        );

        return parameterSuppliersFromTypedData(suppliers);
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new Embedding(source, args.get(0), args.get(1), args.get(2));
    }

    @Override
    protected boolean canSerialize() {
        return false;
    }
}

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
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.FunctionName;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.hamcrest.Matchers;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.esql.core.type.DataType.DENSE_VECTOR;
import static org.hamcrest.Matchers.equalTo;

@FunctionName("text_embedding")
public class TextEmbeddingTests extends AbstractFunctionTestCase {
    @Before
    public void checkCapability() {
        assumeTrue("TEXT_EMBEDDING is not enabled", EsqlCapabilities.Cap.TEXT_EMBEDDING_FUNCTION.isEnabled());
    }

    public TextEmbeddingTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();

        // Test all string type combinations for text input and inference endpoint ID
        for (DataType inputTextDataType : DataType.stringTypes()) {
            for (DataType inferenceIdDataType : DataType.stringTypes()) {
                suppliers.add(
                    new TestCaseSupplier(
                        List.of(inputTextDataType, inferenceIdDataType),
                        () -> new TestCaseSupplier.TestCase(
                            List.of(
                                new TestCaseSupplier.TypedData(randomBytesReference(10).toBytesRef(), inputTextDataType, "inputText"),
                                new TestCaseSupplier.TypedData(randomBytesReference(10).toBytesRef(), inferenceIdDataType, "inference_id")
                            ),
                            Matchers.blankOrNullString(),
                            DENSE_VECTOR,
                            equalTo(true)
                        )
                    )
                );
            }
        }

        return parameterSuppliersFromTypedData(suppliers);
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new TextEmbedding(source, args.get(0), args.get(1));
    }
}

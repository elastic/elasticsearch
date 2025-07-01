/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.inference;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.xpack.core.security.authc.support.mapper.expressiondsl.FieldExpression;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MapExpression;
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
import java.util.stream.Stream;

import static org.elasticsearch.xpack.esql.core.tree.Source.EMPTY;
import static org.elasticsearch.xpack.esql.core.type.DataType.DENSE_VECTOR;
import static org.elasticsearch.xpack.esql.core.type.DataType.UNSUPPORTED;
import static org.hamcrest.Matchers.equalTo;

@FunctionName("text_dense_vector_embedding")
public class DenseVectorEmbeddingFunctionTests extends AbstractFunctionTestCase {
    @Before
    public void checkCapability() {
        assumeTrue("DENSE_VECTOR_EMBEDDING_FUNCTION is not enabled", EsqlCapabilities.Cap.DENSE_VECTOR_EMBEDDING_FUNCTION.isEnabled());
    }

    public DenseVectorEmbeddingFunctionTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<List<TestCaseSupplier.TypedData>> paramList = List.of(List.of());
        List<Supplier<List<TestCaseSupplier.TypedData>>> paramSuppliers = List.of(
            () -> Stream.concat(
                textTestCases("inputText", new BytesRef("input text value")).stream(),
                textTestCases("inputText", new FieldExpression("foo", List.of(new FieldExpression.FieldValue("foo")))).stream()
            ).toList(),
            DenseVectorEmbeddingFunctionTests::optionsTestCases
        );

        for (Supplier<List<TestCaseSupplier.TypedData>> paramSupplier : paramSuppliers) {
            List<List<TestCaseSupplier.TypedData>> newParams = new ArrayList<>();
            for (List<TestCaseSupplier.TypedData> params : paramList) {
                for (TestCaseSupplier.TypedData value : paramSupplier.get()) {
                    List<TestCaseSupplier.TypedData> combination = new ArrayList<>(params);
                    combination.add(value);
                    newParams.add(combination);
                }
            }
            paramList = newParams;
        }

        return parameterSuppliersFromTypedData(paramList.stream().map(args -> {
            List<DataType> dataTypes = args.stream().map(TestCaseSupplier.TypedData::type).toList();
            return new TestCaseSupplier(
                "TextDenseVectorEmbedding[" + dataTypes + "]",
                dataTypes,
                () -> new TestCaseSupplier.TestCase(args, Matchers.blankOrNullString(), DENSE_VECTOR, equalTo(true))
            );
        }).toList());
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new DenseVectorEmbeddingFunction(source, args.get(0), args.get(1));
    }

    private static List<TestCaseSupplier.TypedData> textTestCases(String name, Object value) {
        return DataType.stringTypes().stream().map(dataType -> new TestCaseSupplier.TypedData(value, dataType, name)).toList();
    }

    private static List<TestCaseSupplier.TypedData> optionsTestCases() {
        Literal inferenceIdOptionName = Literal.keyword(EMPTY, "inference_id");
        return DataType.stringTypes()
            .stream()
            .map(
                dataType -> new TestCaseSupplier.TypedData(
                    new MapExpression(EMPTY, List.of(inferenceIdOptionName, new Literal(EMPTY, new BytesRef("inferenceId"), dataType))),
                    UNSUPPORTED,
                    "options"
                )
            )
            .toList();
    }
}

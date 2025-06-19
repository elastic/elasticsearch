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
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MapExpression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.FunctionName;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.hamcrest.Matchers;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.elasticsearch.xpack.esql.core.tree.Source.EMPTY;
import static org.elasticsearch.xpack.esql.core.type.DataType.DOUBLE;
import static org.elasticsearch.xpack.esql.core.type.DataType.UNSUPPORTED;
import static org.hamcrest.Matchers.equalTo;

@FunctionName("rerank")
public class RerankFunctionTests extends AbstractFunctionTestCase {
    public RerankFunctionTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        @SuppressWarnings("unchecked")
        List<TestCaseSupplier> suppliers = crossProduct(
            textTestCases("field", new FieldExpression("foo", List.of(new FieldExpression.FieldValue("foo")))),
            textTestCases("query", new BytesRef("bar")),
            optionsTestCases()
        ).stream().map(args -> {
            List<DataType> dataTypes = args.stream().map(TestCaseSupplier.TypedData::type).toList();
            return new TestCaseSupplier(
                "Rerank(" + dataTypes + ")",
                dataTypes,
                () -> new TestCaseSupplier.TestCase(args, Matchers.blankOrNullString(), DOUBLE, equalTo(true))
            );
        }).toList();

        return parameterSuppliersFromTypedData(suppliers);
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new RerankFunction(source, args.get(0), args.get(1), args.get(2));
    }

    private static List<List<TestCaseSupplier.TypedData>> textTestCases(String name, Object value) {
        return DataType.stringTypes().stream().map(dataType -> List.of(new TestCaseSupplier.TypedData(value, dataType, name))).toList();
    }

    private static List<List<TestCaseSupplier.TypedData>> optionsTestCases() {
        return List.of(
            List.of(new TestCaseSupplier.TypedData(null, UNSUPPORTED, "options")),
            List.of(new TestCaseSupplier.TypedData(new MapExpression(EMPTY, List.of()), UNSUPPORTED, "options")),
            List.of(
                new TestCaseSupplier.TypedData(
                    new MapExpression(EMPTY, List.of(Literal.keyword(EMPTY, "inference_id"), Literal.keyword(EMPTY, "inference_id"))),
                    UNSUPPORTED,
                    "options"
                )
            )
        );
    }

    @SuppressWarnings("unchecked")
    private static List<List<TestCaseSupplier.TypedData>> crossProduct(
        List<List<TestCaseSupplier.TypedData>> left,
        List<List<TestCaseSupplier.TypedData>>... right
    ) {
        if (right.length == 0) {
            return left;
        }

        return crossProduct(crossProduct(left, right[0]), Arrays.copyOfRange(right, 1, right.length));
    }

    private static List<List<TestCaseSupplier.TypedData>> crossProduct(
        List<List<TestCaseSupplier.TypedData>> left,
        List<List<TestCaseSupplier.TypedData>> right
    ) {
        List<List<TestCaseSupplier.TypedData>> result = new ArrayList<>(left.size() + right.size());
        for (int i = 0; i < left.size(); i++) {
            for (int j = 0; j < right.size(); j++) {
                List<TestCaseSupplier.TypedData> leftData = left.get(i);
                List<TestCaseSupplier.TypedData> rightData = right.get(j);
                result.add(Stream.concat(leftData.stream(), rightData.stream()).toList());
            }
        }

        return result;
    }
}

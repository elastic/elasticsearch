/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.convert;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;

public class ToDenseVectorTests extends AbstractScalarFunctionTestCase {

    public ToDenseVectorTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();

        suppliers.add(
            new TestCaseSupplier(
                "int",
                List.of(DataType.INTEGER),
                () -> {
                    List<Integer> data = Arrays.asList(randomArray(1, 10, Integer[]::new, ESTestCase::randomInt));
                    return new TestCaseSupplier.TestCase(
                        List.of(
                            new TestCaseSupplier.TypedData(
                                data,
                                DataType.INTEGER,
                                "int"
                            )
                        ),
                        evaluatorName("Int", "i"),
                        DataType.DENSE_VECTOR,
                        equalTo(data.stream().map(Number::floatValue).toList())
                    );
                }
            )
        );

        suppliers.add(
            new TestCaseSupplier(
                "long",
                List.of(DataType.LONG),
                () -> {
                    List<Long> data = Arrays.asList(randomArray(1, 10, Long[]::new, ESTestCase::randomLong));
                    return new TestCaseSupplier.TestCase(
                        List.of(
                            new TestCaseSupplier.TypedData(
                                data,
                                DataType.LONG,
                                "long"
                            )
                        ),
                        evaluatorName("Long", "l"),
                        DataType.DENSE_VECTOR,
                        equalTo(data.stream().map(Number::floatValue).toList())
                    );
                }
            )
        );

        suppliers.add(
            new TestCaseSupplier(
                "mv_string",
                List.of(DataType.KEYWORD),
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(
                            List.of(new BytesRef("1.1"), new BytesRef("2.2")),
                            DataType.KEYWORD,
                            "mv_string"
                        )
                    ),
                    evaluatorName("String", "in"),
                    DataType.DENSE_VECTOR,
                    equalTo(List.of(1.1f, 2.2f))
                )
            )
        );

        return parameterSuppliersFromTypedDataWithDefaultChecksNoErrors(true, suppliers);
    }

    private static String evaluatorName(String inner, String next) {
        String read = "Attribute[channel=0]";
        return "ToDenseVectorFrom" + inner + "Evaluator[" + next + "=" + read + "]";
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new ToDenseVector(source, args.get(0));
    }
}

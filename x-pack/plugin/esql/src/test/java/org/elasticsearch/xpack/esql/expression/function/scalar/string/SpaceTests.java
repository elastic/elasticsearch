/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.string;

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
import static org.hamcrest.Matchers.nullValue;

public class SpaceTests extends AbstractScalarFunctionTestCase {
    public SpaceTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {

        List<TestCaseSupplier> cases = new ArrayList<>();

        cases.add(new TestCaseSupplier("Space basic test", List.of(DataType.INTEGER), () -> {
            int number = between(0, 10);
            return new TestCaseSupplier.TestCase(
                List.of(new TestCaseSupplier.TypedData(number, DataType.INTEGER, "number")),
                "SpaceEvaluator[number=Attribute[channel=0]]",
                DataType.KEYWORD,
                equalTo(new BytesRef(" ".repeat(number)))
            );
        }));

        cases.add(new TestCaseSupplier("Space with number zero", List.of(DataType.INTEGER), () -> {
            int number = 0;
            return new TestCaseSupplier.TestCase(
                List.of(new TestCaseSupplier.TypedData(number, DataType.INTEGER, "number")),
                "SpaceEvaluator[number=Attribute[channel=0]]",
                DataType.KEYWORD,
                equalTo(new BytesRef(""))
            );
        }));

        cases.add(new TestCaseSupplier("Space with negative number", List.of(DataType.INTEGER), () -> {
            int number = randomIntBetween(-10, -1);
            return new TestCaseSupplier.TestCase(
                List.of(new TestCaseSupplier.TypedData(number, DataType.INTEGER, "number")),
                "SpaceEvaluator[number=Attribute[channel=0]]",
                DataType.KEYWORD,
                nullValue()
            ).withWarning("Line -1:-1: evaluation of [] failed, treating result as null. Only first 20 failures recorded.")
                .withWarning("Line -1:-1: java.lang.IllegalArgumentException: Number parameter cannot be negative, found [" + number + "]")
                .withFoldingException(IllegalArgumentException.class, "Number parameter cannot be negative, found [" + number + "]");
        }));

        cases = anyNullIsNull(true, cases);
        cases = errorsForCasesWithoutExamples(cases, (v, p) -> "integer");
        return parameterSuppliersFromTypedData(cases);
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new Space(source, args.get(0));
    }
}

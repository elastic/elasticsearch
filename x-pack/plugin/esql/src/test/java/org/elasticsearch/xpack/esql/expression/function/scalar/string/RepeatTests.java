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

public class RepeatTests extends AbstractScalarFunctionTestCase {
    public RepeatTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {

        List<TestCaseSupplier> cases = new ArrayList<>();

        cases.add(new TestCaseSupplier("Repeat basic test", List.of(DataType.KEYWORD, DataType.INTEGER), () -> {
            String text = randomAlphaOfLength(10);
            int number = between(0, 10);
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(new BytesRef(text), DataType.KEYWORD, "str"),
                    new TestCaseSupplier.TypedData(number, DataType.INTEGER, "number")
                ),
                "RepeatEvaluator[str=Attribute[channel=0], number=Attribute[channel=1]]",
                DataType.KEYWORD,
                equalTo(new BytesRef(text.repeat(number)))
            );
        }));

        cases.add(new TestCaseSupplier("Repeat basic test with text input", List.of(DataType.TEXT, DataType.INTEGER), () -> {
            String text = randomAlphaOfLength(10);
            int number = between(0, 10);
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(new BytesRef(text), DataType.TEXT, "str"),
                    new TestCaseSupplier.TypedData(number, DataType.INTEGER, "number")
                ),
                "RepeatEvaluator[str=Attribute[channel=0], number=Attribute[channel=1]]",
                DataType.KEYWORD,
                equalTo(new BytesRef(text.repeat(number)))
            );
        }));

        cases.add(new TestCaseSupplier("Repeat with number zero", List.of(DataType.KEYWORD, DataType.INTEGER), () -> {
            String text = randomAlphaOfLength(10);
            int number = 0;
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(new BytesRef(text), DataType.KEYWORD, "str"),
                    new TestCaseSupplier.TypedData(number, DataType.INTEGER, "number")
                ),
                "RepeatEvaluator[str=Attribute[channel=0], number=Attribute[channel=1]]",
                DataType.KEYWORD,
                equalTo(new BytesRef(""))
            );
        }));

        cases.add(new TestCaseSupplier("Repeat Unicode", List.of(DataType.KEYWORD, DataType.INTEGER), () -> {
            String text = randomUnicodeOfLength(10);
            int number = randomIntBetween(0, 10);
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(new BytesRef(text), DataType.KEYWORD, "str"),
                    new TestCaseSupplier.TypedData(number, DataType.INTEGER, "number")
                ),
                "RepeatEvaluator[str=Attribute[channel=0], number=Attribute[channel=1]]",
                DataType.KEYWORD,
                equalTo(new BytesRef(text.repeat(number)))
            );
        }));

        cases.add(new TestCaseSupplier("Repeat Negative Number", List.of(DataType.KEYWORD, DataType.INTEGER), () -> {
            String text = randomAlphaOfLength(10);
            int number = randomIntBetween(-10, -1);
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(new BytesRef(text), DataType.KEYWORD, "str"),
                    new TestCaseSupplier.TypedData(number, DataType.INTEGER, "number")
                ),
                "RepeatEvaluator[str=Attribute[channel=0], number=Attribute[channel=1]]",
                DataType.KEYWORD,
                nullValue()
            ).withWarning("Line 1:1: evaluation of [source] failed, treating result as null. Only first 20 failures recorded.")
                .withWarning("Line 1:1: java.lang.IllegalArgumentException: Number parameter cannot be negative, found [" + number + "]")
                .withFoldingException(IllegalArgumentException.class, "Number parameter cannot be negative, found [" + number + "]");
        }));

        return parameterSuppliersFromTypedDataWithDefaultChecksNoErrors(true, cases);
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new Repeat(source, args.get(0), args.get(1));
    }
}

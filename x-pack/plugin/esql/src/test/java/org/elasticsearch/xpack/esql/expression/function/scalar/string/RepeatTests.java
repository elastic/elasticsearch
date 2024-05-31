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
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.xpack.esql.EsqlClientException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataTypes;
import org.elasticsearch.xpack.esql.expression.function.AbstractFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;

public class RepeatTests extends AbstractFunctionTestCase {
    public RepeatTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {

        List<TestCaseSupplier> cases = new ArrayList<>();

        cases.add(new TestCaseSupplier("Repeat basic test", List.of(DataTypes.KEYWORD, DataTypes.INTEGER), () -> {
            String text = randomAlphaOfLength(10);
            int number = between(0, 10);
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(new BytesRef(text), DataTypes.KEYWORD, "str"),
                    new TestCaseSupplier.TypedData(number, DataTypes.INTEGER, "number")
                ),
                "RepeatEvaluator[str=Attribute[channel=0], number=Attribute[channel=1]]",
                DataTypes.KEYWORD,
                equalTo(new BytesRef(text.repeat(number)))
            );
        }));

        cases.add(new TestCaseSupplier("Repeat basic test with text input", List.of(DataTypes.TEXT, DataTypes.INTEGER), () -> {
            String text = randomAlphaOfLength(10);
            int number = between(0, 10);
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(new BytesRef(text), DataTypes.TEXT, "str"),
                    new TestCaseSupplier.TypedData(number, DataTypes.INTEGER, "number")
                ),
                "RepeatEvaluator[str=Attribute[channel=0], number=Attribute[channel=1]]",
                DataTypes.KEYWORD,
                equalTo(new BytesRef(text.repeat(number)))
            );
        }));

        cases.add(new TestCaseSupplier("Repeat with number zero", List.of(DataTypes.KEYWORD, DataTypes.INTEGER), () -> {
            String text = randomAlphaOfLength(10);
            int number = 0;
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(new BytesRef(text), DataTypes.KEYWORD, "str"),
                    new TestCaseSupplier.TypedData(number, DataTypes.INTEGER, "number")
                ),
                "RepeatEvaluator[str=Attribute[channel=0], number=Attribute[channel=1]]",
                DataTypes.KEYWORD,
                equalTo(new BytesRef(""))
            );
        }));

        cases.add(new TestCaseSupplier("Repeat Unicode", List.of(DataTypes.KEYWORD, DataTypes.INTEGER), () -> {
            String text = randomUnicodeOfLength(10);
            int number = randomIntBetween(0, 10);
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(new BytesRef(text), DataTypes.KEYWORD, "str"),
                    new TestCaseSupplier.TypedData(number, DataTypes.INTEGER, "number")
                ),
                "RepeatEvaluator[str=Attribute[channel=0], number=Attribute[channel=1]]",
                DataTypes.KEYWORD,
                equalTo(new BytesRef(text.repeat(number)))
            );
        }));

        cases.add(new TestCaseSupplier("Repeat Negative Number", List.of(DataTypes.KEYWORD, DataTypes.INTEGER), () -> {
            String text = randomAlphaOfLength(10);
            int number = randomIntBetween(-10, -1);
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(new BytesRef(text), DataTypes.KEYWORD, "str"),
                    new TestCaseSupplier.TypedData(number, DataTypes.INTEGER, "number")
                ),
                "RepeatEvaluator[str=Attribute[channel=0], number=Attribute[channel=1]]",
                DataTypes.KEYWORD,
                nullValue()
            ).withWarning("Line -1:-1: evaluation of [] failed, treating result as null. Only first 20 failures recorded.")
                .withWarning("Line -1:-1: java.lang.IllegalArgumentException: Number parameter cannot be negative, found [" + number + "]");
        }));

        cases = anyNullIsNull(true, cases);
        cases = errorsForCasesWithoutExamples(cases);
        return parameterSuppliersFromTypedData(cases);
    }

    public void testAlmostTooBig() {
        String str = randomAlphaOfLength(1);
        int number = (int) Repeat.MAX_REPEATED_LENGTH;
        String repeated = process(str, number);
        assertThat(repeated, equalTo(str.repeat(number)));
    }

    public void testTooBig() {
        String str = randomAlphaOfLength(1);
        int number = (int) Repeat.MAX_REPEATED_LENGTH + 1;
        Exception e = expectThrows(EsqlClientException.class, () -> process(str, number));
        assertThat(e.getMessage(), startsWith("Creating repeated strings with more than [1048576] bytes is not supported"));
    }

    public String process(String str, int number) {
        try (
            var eval = evaluator(new Repeat(Source.EMPTY, field("string", DataTypes.KEYWORD), field("number", DataTypes.INTEGER))).get(
                driverContext()
            );
            Block block = eval.eval(row(List.of(new BytesRef(str), number)));
        ) {
            return ((BytesRef) BlockUtils.toJavaObject(block, 0)).utf8ToString();
        }
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new Repeat(source, args.get(0), args.get(1));
    }
}

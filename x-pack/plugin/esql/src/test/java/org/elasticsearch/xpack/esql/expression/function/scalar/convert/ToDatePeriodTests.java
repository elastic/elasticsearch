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
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.FunctionName;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter;

import java.time.Period;
import java.time.temporal.TemporalAmount;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.randomLiteral;
import static org.elasticsearch.xpack.esql.core.type.DataType.DATE_PERIOD;
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.elasticsearch.xpack.esql.core.type.DataType.TEXT;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.DATE_PERIODS;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.matchesPattern;

@FunctionName("to_dateperiod")
public class ToDatePeriodTests extends AbstractScalarFunctionTestCase {
    public ToDatePeriodTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();

        suppliers.add(new TestCaseSupplier(List.of(DATE_PERIOD), () -> {
            Period field = (Period) randomLiteral(DATE_PERIOD).value();
            return new TestCaseSupplier.TestCase(
                List.of(new TestCaseSupplier.TypedData(field, DATE_PERIOD, "field").forceLiteral()),
                matchesPattern("LiteralsEvaluator.*"),
                DATE_PERIOD,
                equalTo(field)
            ).withoutEvaluator();
        }));

        for (EsqlDataTypeConverter.INTERVALS interval : DATE_PERIODS) {
            for (DataType inputType : List.of(KEYWORD, TEXT)) {
                suppliers.add(new TestCaseSupplier(List.of(inputType), () -> {
                    BytesRef field = new BytesRef(
                        " ".repeat(randomIntBetween(0, 10)) + (randomBoolean() ? "" : "-") + randomIntBetween(0, 36500000) + " ".repeat(
                            randomIntBetween(1, 10)
                        ) + interval.toString() + " ".repeat(randomIntBetween(0, 10))
                    );
                    TemporalAmount result = EsqlDataTypeConverter.parseTemporalAmount(field.utf8ToString(), DATE_PERIOD);
                    return new TestCaseSupplier.TestCase(
                        List.of(new TestCaseSupplier.TypedData(field, inputType, "field").forceLiteral()),
                        matchesPattern("LiteralsEvaluator.*"),
                        DATE_PERIOD,
                        equalTo(result)
                    ).withoutEvaluator();
                }));
            }
        }
        return parameterSuppliersFromTypedDataWithDefaultChecksNoErrors(true, suppliers);
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new ToDatePeriod(source, args.get(0));
    }

    @Override
    public void testSerializationOfSimple() {
        assertTrue("Serialization test does not apply", true);
    }

    @Override
    protected Expression serializeDeserializeExpression(Expression expression) {
        // Can't be serialized
        return expression;
    }
}

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

import java.time.Duration;
import java.time.temporal.TemporalAmount;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.randomLiteral;
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.elasticsearch.xpack.esql.core.type.DataType.TEXT;
import static org.elasticsearch.xpack.esql.core.type.DataType.TIME_DURATION;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.TIME_DURATIONS;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.matchesPattern;

@FunctionName("to_timeduration")
public class ToTimeDurationTests extends AbstractScalarFunctionTestCase {
    public ToTimeDurationTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();

        suppliers.add(new TestCaseSupplier(List.of(TIME_DURATION), () -> {
            Duration field = (Duration) randomLiteral(TIME_DURATION).value();
            return new TestCaseSupplier.TestCase(
                List.of(new TestCaseSupplier.TypedData(field, TIME_DURATION, "field").forceLiteral()),
                matchesPattern("LiteralsEvaluator.*"),
                TIME_DURATION,
                equalTo(field)
            ).withoutEvaluator();
        }));

        for (EsqlDataTypeConverter.INTERVALS interval : TIME_DURATIONS) {
            for (DataType inputType : List.of(KEYWORD, TEXT)) {
                suppliers.add(new TestCaseSupplier(List.of(inputType), () -> {
                    BytesRef field = new BytesRef(
                        " ".repeat(randomIntBetween(0, 10)) + (randomBoolean() ? "" : "-") + randomIntBetween(0, Integer.MAX_VALUE) + " "
                            .repeat(randomIntBetween(1, 10)) + interval.toString() + " ".repeat(randomIntBetween(0, 10))
                    );
                    TemporalAmount result = EsqlDataTypeConverter.parseTemporalAmount(field.utf8ToString(), TIME_DURATION);
                    return new TestCaseSupplier.TestCase(
                        List.of(new TestCaseSupplier.TypedData(field, inputType, "field").forceLiteral()),
                        matchesPattern("LiteralsEvaluator.*"),
                        TIME_DURATION,
                        equalTo(result)
                    ).withoutEvaluator();
                }));
            }
        }
        return parameterSuppliersFromTypedDataWithDefaultChecksNoErrors(true, suppliers);
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new ToTimeDuration(source, args.get(0));
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

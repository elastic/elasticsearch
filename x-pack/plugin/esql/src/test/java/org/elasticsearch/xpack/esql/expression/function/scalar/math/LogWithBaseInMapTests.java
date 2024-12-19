/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.math;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.xpack.esql.core.expression.EntryExpression;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MapExpression;
import org.elasticsearch.xpack.esql.core.expression.predicate.operator.math.Maths;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.elasticsearch.xpack.esql.expression.function.scalar.map.LogWithBaseInMap;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.esql.core.type.DataType.DOUBLE;
import static org.elasticsearch.xpack.esql.core.type.DataType.INTEGER;
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.elasticsearch.xpack.esql.core.type.DataType.LONG;
import static org.elasticsearch.xpack.esql.core.type.DataType.UNSUPPORTED;
import static org.hamcrest.Matchers.equalTo;

public class LogWithBaseInMapTests extends AbstractScalarFunctionTestCase {
    public LogWithBaseInMapTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();
        ints(suppliers);
        longs(suppliers);
        doubles(suppliers);
        // Add null cases before the rest of the error cases, so messages are correct.
        // suppliers = anyNullIsNull(true, suppliers);
        // Negative cases

        // return parameterSuppliersFromTypedData(errorsForCasesWithoutExamples(suppliers, (v, p) -> "numeric"));
        return parameterSuppliersFromTypedData(suppliers);
    }

    private static void ints(List<TestCaseSupplier> suppliers) {
        TestCaseSupplier supplier = new TestCaseSupplier(List.of(INTEGER), () -> {
            int number = randomIntBetween(2, 100);
            int base = randomIntBetween(2, 100);
            EntryExpression entry = new EntryExpression(
                Source.EMPTY,
                new Literal(Source.EMPTY, "base", KEYWORD),
                new Literal(Source.EMPTY, base, INTEGER)
            );
            List<TestCaseSupplier.TypedData> values = new ArrayList<>();
            values.add(new TestCaseSupplier.TypedData(number, INTEGER, "number"));
            values.add(new TestCaseSupplier.TypedData(new MapExpression(Source.EMPTY, List.of(entry)), UNSUPPORTED, "base").forceLiteral());
            return new TestCaseSupplier.TestCase(
                values,
                "LogWithBaseInMapEvaluator[value=CastIntToDoubleEvaluator[v=Attribute[channel=0]], base=" + (double) base + "]",
                DOUBLE,
                equalTo(Math.log10(number) / Math.log10(base))
            );
        });
        suppliers.add(supplier);
    }

    private static void longs(List<TestCaseSupplier> suppliers) {
        TestCaseSupplier supplier = new TestCaseSupplier(List.of(LONG), () -> {
            long number = randomLongBetween(2L, 100L);
            long base = randomLongBetween(2L, 100L);
            EntryExpression entry = new EntryExpression(
                Source.EMPTY,
                new Literal(Source.EMPTY, "base", KEYWORD),
                new Literal(Source.EMPTY, base, LONG)
            );
            List<TestCaseSupplier.TypedData> values = new ArrayList<>();
            values.add(new TestCaseSupplier.TypedData(number, LONG, "number"));
            values.add(new TestCaseSupplier.TypedData(new MapExpression(Source.EMPTY, List.of(entry)), UNSUPPORTED, "base").forceLiteral());
            return new TestCaseSupplier.TestCase(
                values,
                "LogWithBaseInMapEvaluator[value=CastLongToDoubleEvaluator[v=Attribute[channel=0]], base=" + (double) base + "]",
                DOUBLE,
                equalTo(Math.log10(number) / Math.log10(base))
            );
        });
        suppliers.add(supplier);
    }

    private static void doubles(List<TestCaseSupplier> suppliers) {
        TestCaseSupplier supplier = new TestCaseSupplier(List.of(DOUBLE), () -> {
            double number = Maths.round(randomDoubleBetween(2d, 100d, true), 2).doubleValue();
            double base = Maths.round(randomDoubleBetween(2d, 100d, true), 2).doubleValue();
            EntryExpression entry = new EntryExpression(
                Source.EMPTY,
                new Literal(Source.EMPTY, "base", KEYWORD),
                new Literal(Source.EMPTY, base, DOUBLE)
            );
            List<TestCaseSupplier.TypedData> values = new ArrayList<>();
            values.add(new TestCaseSupplier.TypedData(number, DOUBLE, "number"));
            values.add(new TestCaseSupplier.TypedData(new MapExpression(Source.EMPTY, List.of(entry)), UNSUPPORTED, "base").forceLiteral());
            return new TestCaseSupplier.TestCase(
                values,
                "LogWithBaseInMapEvaluator[value=Attribute[channel=0], base=" + base + "]",
                DOUBLE,
                equalTo(Math.log10(number) / Math.log10(base))
            );
        });
        suppliers.add(supplier);
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        // return new LogWithBaseInMap(source, args.get(0), randomBoolean() ? randomMapExpression() : null);
        return new LogWithBaseInMap(source, args.get(0), args.size() > 1 ? args.get(1) : null);
    }

    private static MapExpression randomMapExpression() {
        double base = randomDoubleBetween(2d, Double.MAX_VALUE, true);
        EntryExpression entry = new EntryExpression(
            Source.EMPTY,
            new Literal(Source.EMPTY, "base", KEYWORD),
            new Literal(Source.EMPTY, base, DOUBLE)
        );
        return new MapExpression(Source.EMPTY, List.of(entry));
    }
}

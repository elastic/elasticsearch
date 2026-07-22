/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.multivalue;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Lambda;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class MvFilterTests extends AbstractScalarFunctionTestCase {
    public MvFilterTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> cases = new ArrayList<>();

        // Typed comparison: x -> x > 5
        cases.add(new TestCaseSupplier("filter(int mv, x -> x > 5) keeps matching", List.of(DataType.INTEGER, DataType.LAMBDA), () -> {
            ReferenceAttribute param = new ReferenceAttribute(Source.EMPTY, "x", DataType.INTEGER);
            Lambda lambda = new Lambda(
                Source.EMPTY,
                List.of(param, new GreaterThan(Source.EMPTY, param, new Literal(Source.EMPTY, 5, DataType.INTEGER), null))
            );
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(List.of(1, 7, 8, 2), DataType.INTEGER, "field"),
                    new TestCaseSupplier.TypedData(lambda, DataType.LAMBDA, "predicate").forceLiteral()
                ),
                containsString("MvFilterIntEvaluator[field=Attribute[channel=0]"),
                DataType.INTEGER,
                equalTo(List.of(7, 8))
            );
        }));
        cases.add(new TestCaseSupplier("filter(int mv, x -> x > 5) none match = null", List.of(DataType.INTEGER, DataType.LAMBDA), () -> {
            ReferenceAttribute param = new ReferenceAttribute(Source.EMPTY, "x", DataType.INTEGER);
            Lambda lambda = new Lambda(
                Source.EMPTY,
                List.of(param, new GreaterThan(Source.EMPTY, param, new Literal(Source.EMPTY, 5, DataType.INTEGER), null))
            );
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(List.of(1, 2, 3), DataType.INTEGER, "field"),
                    new TestCaseSupplier.TypedData(lambda, DataType.LAMBDA, "predicate").forceLiteral()
                ),
                containsString("MvFilterIntEvaluator[field=Attribute[channel=0]"),
                DataType.INTEGER,
                nullValue()
            );
        }));

        // Constant true predicate keeps all elements, for all supported field types.
        // filter([v1, v2], x -> true) = [v1, v2]
        addConstantTrueCases(cases, DataType.BOOLEAN, List.of(true, false), "MvFilterBooleanEvaluator");
        addConstantTrueCases(cases, DataType.LONG, List.of(1L, 2L), "MvFilterLongEvaluator");
        addConstantTrueCases(cases, DataType.DOUBLE, List.of(1.0, 2.0), "MvFilterDoubleEvaluator");
        addConstantTrueCases(cases, DataType.DATETIME, List.of(1000L, 2000L), "MvFilterLongEvaluator");
        addConstantTrueCases(cases, DataType.DATE_NANOS, List.of(1000L, 2000L), "MvFilterLongEvaluator");
        addConstantTrueCases(cases, DataType.UNSIGNED_LONG, List.of(BigInteger.ONE, BigInteger.valueOf(2)), "MvFilterLongEvaluator");
        addConstantTrueCases(cases, DataType.KEYWORD, List.of(new BytesRef("a"), new BytesRef("b")), "MvFilterBytesRefEvaluator");
        addConstantTrueCases(cases, DataType.TEXT, List.of(new BytesRef("a"), new BytesRef("b")), "MvFilterBytesRefEvaluator");
        addConstantTrueCases(
            cases,
            DataType.IP,
            List.of(new BytesRef(new byte[16]), new BytesRef(new byte[16])),
            "MvFilterBytesRefEvaluator"
        );
        addConstantTrueCases(cases, DataType.VERSION, List.of(new BytesRef("1.0"), new BytesRef("2.0")), "MvFilterBytesRefEvaluator");
        addConstantTrueCases(cases, DataType.FLATTENED, List.of(new BytesRef("a"), new BytesRef("b")), "MvFilterBytesRefEvaluator");
        addConstantTrueCases(cases, DataType.GEO_POINT, List.of(new BytesRef("geo"), new BytesRef("geo2")), "MvFilterBytesRefEvaluator");
        addConstantTrueCases(
            cases,
            DataType.GEO_SHAPE,
            List.of(new BytesRef("shape"), new BytesRef("shape2")),
            "MvFilterBytesRefEvaluator"
        );
        addConstantTrueCases(
            cases,
            DataType.CARTESIAN_POINT,
            List.of(new BytesRef("cp"), new BytesRef("cp2")),
            "MvFilterBytesRefEvaluator"
        );
        addConstantTrueCases(
            cases,
            DataType.CARTESIAN_SHAPE,
            List.of(new BytesRef("cs"), new BytesRef("cs2")),
            "MvFilterBytesRefEvaluator"
        );

        return parameterSuppliersFromTypedData(cases);
    }

    private static void addConstantTrueCases(List<TestCaseSupplier> cases, DataType fieldType, List<Object> values, String evaluatorName) {
        addConstantTrueCases(cases, fieldType, values, values, evaluatorName);
    }

    private static void addConstantTrueCases(
        List<TestCaseSupplier> cases,
        DataType fieldType,
        List<Object> fieldValues,
        List<Object> expectedValues,
        String evaluatorName
    ) {
        cases.add(
            new TestCaseSupplier("filter(" + fieldType.typeName() + " mv, x -> true) = all", List.of(fieldType, DataType.LAMBDA), () -> {
                ReferenceAttribute param = new ReferenceAttribute(Source.EMPTY, "x", fieldType);
                Lambda lambda = new Lambda(Source.EMPTY, List.of(param, new Literal(Source.EMPTY, true, DataType.BOOLEAN)));
                return new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(fieldValues, fieldType, "field"),
                        new TestCaseSupplier.TypedData(lambda, DataType.LAMBDA, "predicate").forceLiteral()
                    ),
                    containsString(evaluatorName + "[field=Attribute[channel=0]"),
                    fieldType,
                    equalTo(expectedValues)
                );
            })
        );
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new MvFilter(source, args.get(0), (Lambda) args.get(1));
    }

    @Override
    public void testFold() {
        // Lambda expressions are not foldable; this test does not apply.
        assumeTrue("Lambda functions are not foldable", false);
    }
}

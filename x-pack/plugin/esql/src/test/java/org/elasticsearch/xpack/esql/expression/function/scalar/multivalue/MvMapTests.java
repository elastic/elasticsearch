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
import org.elasticsearch.xpack.esql.expression.function.FunctionName;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Mul;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

@FunctionName("map")
public class MvMapTests extends AbstractScalarFunctionTestCase {
    public MvMapTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> cases = new ArrayList<>();

        // Arithmetic transform: x -> x * 10
        cases.add(new TestCaseSupplier("map(int mv, x -> x * 10)", List.of(DataType.INTEGER, DataType.LAMBDA), () -> {
            ReferenceAttribute param = new ReferenceAttribute(Source.EMPTY, "x", DataType.INTEGER);
            Lambda lambda = new Lambda(
                Source.EMPTY,
                List.of(param, new Mul(Source.EMPTY, param, new Literal(Source.EMPTY, 10, DataType.INTEGER)))
            );
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(List.of(1, 2, 3), DataType.INTEGER, "field"),
                    new TestCaseSupplier.TypedData(lambda, DataType.LAMBDA, "transform").forceLiteral()
                ),
                containsString("MvMapIntEvaluator[field=Attribute[channel=0]"),
                DataType.INTEGER,
                equalTo(List.of(10, 20, 30))
            );
        }));
        cases.add(new TestCaseSupplier("map(int single, x -> x * 2)", List.of(DataType.INTEGER, DataType.LAMBDA), () -> {
            ReferenceAttribute param = new ReferenceAttribute(Source.EMPTY, "x", DataType.INTEGER);
            Lambda lambda = new Lambda(
                Source.EMPTY,
                List.of(param, new Mul(Source.EMPTY, param, new Literal(Source.EMPTY, 2, DataType.INTEGER)))
            );
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(5, DataType.INTEGER, "field"),
                    new TestCaseSupplier.TypedData(lambda, DataType.LAMBDA, "transform").forceLiteral()
                ),
                containsString("MvMapIntEvaluator[field=Attribute[channel=0]"),
                DataType.INTEGER,
                equalTo(10)
            );
        }));

        // Identity transform (x -> x) for all supported field types.
        // map([v1, v2], x -> x) = [v1, v2]
        addIdentityCases(cases, DataType.BOOLEAN, List.of(true, false), "MvMapBooleanEvaluator");
        addIdentityCases(cases, DataType.LONG, List.of(1L, 2L), "MvMapLongEvaluator");
        addIdentityCases(cases, DataType.DOUBLE, List.of(1.0, 2.0), "MvMapDoubleEvaluator");
        addIdentityCases(cases, DataType.DATETIME, List.of(1000L, 2000L), "MvMapLongEvaluator");
        addIdentityCases(cases, DataType.DATE_NANOS, List.of(1000L, 2000L), "MvMapLongEvaluator");
        addIdentityCases(cases, DataType.UNSIGNED_LONG, List.of(BigInteger.ONE, BigInteger.valueOf(2)), "MvMapLongEvaluator");
        addIdentityCases(cases, DataType.KEYWORD, List.of(new BytesRef("a"), new BytesRef("b")), "MvMapBytesRefEvaluator");
        addIdentityCases(cases, DataType.TEXT, List.of(new BytesRef("a"), new BytesRef("b")), "MvMapBytesRefEvaluator");
        addIdentityCases(cases, DataType.IP, List.of(new BytesRef(new byte[16]), new BytesRef(new byte[16])), "MvMapBytesRefEvaluator");
        addIdentityCases(cases, DataType.VERSION, List.of(new BytesRef("1.0"), new BytesRef("2.0")), "MvMapBytesRefEvaluator");
        addIdentityCases(cases, DataType.FLATTENED, List.of(new BytesRef("a"), new BytesRef("b")), "MvMapBytesRefEvaluator");
        addIdentityCases(cases, DataType.GEO_POINT, List.of(new BytesRef("geo"), new BytesRef("geo2")), "MvMapBytesRefEvaluator");
        addIdentityCases(cases, DataType.GEO_SHAPE, List.of(new BytesRef("shape"), new BytesRef("shape2")), "MvMapBytesRefEvaluator");
        addIdentityCases(cases, DataType.CARTESIAN_POINT, List.of(new BytesRef("cp"), new BytesRef("cp2")), "MvMapBytesRefEvaluator");
        addIdentityCases(cases, DataType.CARTESIAN_SHAPE, List.of(new BytesRef("cs"), new BytesRef("cs2")), "MvMapBytesRefEvaluator");

        return parameterSuppliersFromTypedData(cases);
    }

    private static void addIdentityCases(List<TestCaseSupplier> cases, DataType fieldType, List<Object> values, String evaluatorName) {
        addIdentityCases(cases, fieldType, values, values, evaluatorName);
    }

    private static void addIdentityCases(
        List<TestCaseSupplier> cases,
        DataType fieldType,
        List<Object> fieldValues,
        List<Object> expectedValues,
        String evaluatorName
    ) {
        cases.add(new TestCaseSupplier("map(" + fieldType.typeName() + " mv, x -> x)", List.of(fieldType, DataType.LAMBDA), () -> {
            ReferenceAttribute param = new ReferenceAttribute(Source.EMPTY, "x", fieldType);
            // Identity lambda: x -> x (body is the parameter reference itself)
            Lambda lambda = new Lambda(Source.EMPTY, List.of(param, param));
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(fieldValues, fieldType, "field"),
                    new TestCaseSupplier.TypedData(lambda, DataType.LAMBDA, "transform").forceLiteral()
                ),
                containsString(evaluatorName + "[field=Attribute[channel=0]"),
                fieldType,
                equalTo(expectedValues)
            );
        }));
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new MvMap(source, args.get(0), (Lambda) args.get(1));
    }

    @Override
    public void testFold() {
        // Lambda expressions are not foldable; this test does not apply.
        assumeTrue("Lambda functions are not foldable", false);
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic;

import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;

import static org.elasticsearch.test.ESTestCase.between;
import static org.elasticsearch.test.ESTestCase.randomDouble;
import static org.elasticsearch.test.ESTestCase.randomDoubleBetween;
import static org.elasticsearch.test.ESTestCase.randomInt;
import static org.elasticsearch.test.ESTestCase.randomLong;
import static org.elasticsearch.xpack.esql.core.type.DataType.DENSE_VECTOR;
import static org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier.randomDenseVector;
import static org.hamcrest.Matchers.equalTo;

public class DenseVectorTestCaseHelper {

    static List<TestCaseSupplier> denseVectorScalarCases(
        String opName,
        BiFunction<List<Float>, Number, List<Float>> vectorScalarOp,
        BiFunction<Number, List<Float>, List<Float>> scalarVectorOp
    ) {
        List<TestCaseSupplier> suppliers = new ArrayList<>();

        // Vector + Integer
        suppliers.add(new TestCaseSupplier(List.of(DENSE_VECTOR, DataType.INTEGER), () -> {
            int dimensions = between(64, 128);
            List<Float> vector = randomDenseVector(dimensions);
            int scalar = randomInt();
            List<Float> expected = vectorScalarOp.apply(vector, scalar);
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(vector, DENSE_VECTOR, "vector"),
                    new TestCaseSupplier.TypedData(scalar, DataType.INTEGER, "scalar").forceLiteral()
                ),
                "DenseVectorScalarEvaluator[lhs=Attribute[channel=0], rhs=scalar_constant, opName=" + opName + "]",
                DENSE_VECTOR,
                equalTo(expected)
            );
        }));

        // Integer + Vector
        suppliers.add(new TestCaseSupplier(List.of(DataType.INTEGER, DENSE_VECTOR), () -> {
            int dimensions = between(64, 128);
            List<Float> vector = randomDenseVector(dimensions);
            int scalar = randomInt();
            List<Float> expected = scalarVectorOp.apply(scalar, vector);
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(scalar, DataType.INTEGER, "scalar").forceLiteral(),
                    new TestCaseSupplier.TypedData(vector, DENSE_VECTOR, "vector")
                ),
                "DenseVectorScalarEvaluator[lhs=Attribute[channel=0], rhs=scalar_constant, opName=" + opName + "]",
                DENSE_VECTOR,
                equalTo(expected)
            );
        }));

        // Vector + Long
        suppliers.add(new TestCaseSupplier(List.of(DENSE_VECTOR, DataType.LONG), () -> {
            int dimensions = between(64, 128);
            List<Float> vector = randomDenseVector(dimensions);
            long scalar = randomLong();
            List<Float> expected = vectorScalarOp.apply(vector, scalar);
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(vector, DENSE_VECTOR, "vector"),
                    new TestCaseSupplier.TypedData(scalar, DataType.LONG, "scalar").forceLiteral()
                ),
                "DenseVectorScalarEvaluator[lhs=Attribute[channel=0], rhs=scalar_constant, opName=" + opName + "]",
                DENSE_VECTOR,
                equalTo(expected)
            );
        }));

        // Long + Vector
        suppliers.add(new TestCaseSupplier(List.of(DataType.LONG, DENSE_VECTOR), () -> {
            int dimensions = between(64, 128);
            List<Float> vector = randomDenseVector(dimensions);
            long scalar = randomLong();
            List<Float> expected = scalarVectorOp.apply(scalar, vector);
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(scalar, DataType.LONG, "scalar").forceLiteral(),
                    new TestCaseSupplier.TypedData(vector, DENSE_VECTOR, "vector")
                ),
                "DenseVectorScalarEvaluator[lhs=Attribute[channel=0], rhs=scalar_constant, opName=" + opName + "]",
                DENSE_VECTOR,
                equalTo(expected)
            );
        }));

        // Vector + Double
        suppliers.add(new TestCaseSupplier(List.of(DENSE_VECTOR, DataType.DOUBLE), () -> {
            int dimensions = between(64, 128);
            List<Float> vector = randomDenseVector(dimensions);
            double scalar = randomDouble();
            List<Float> expected = vectorScalarOp.apply(vector, scalar);
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(vector, DENSE_VECTOR, "vector"),
                    new TestCaseSupplier.TypedData(scalar, DataType.DOUBLE, "scalar").forceLiteral()
                ),
                "DenseVectorScalarEvaluator[lhs=Attribute[channel=0], rhs=scalar_constant, opName=" + opName + "]",
                DENSE_VECTOR,
                equalTo(expected)
            );
        }));

        // Double + Vector
        suppliers.add(new TestCaseSupplier(List.of(DataType.DOUBLE, DENSE_VECTOR), () -> {
            int dimensions = between(64, 128);
            List<Float> vector = randomDenseVector(dimensions);
            double scalar = randomDoubleBetween(-1.0, 1.0, true);
            List<Float> expected = scalarVectorOp.apply(scalar, vector);
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(scalar, DataType.DOUBLE, "scalar").forceLiteral(),
                    new TestCaseSupplier.TypedData(vector, DENSE_VECTOR, "vector")
                ),
                "DenseVectorScalarEvaluator[lhs=Attribute[channel=0], rhs=scalar_constant, opName=" + opName + "]",
                DENSE_VECTOR,
                equalTo(expected)
            );
        }));

        return suppliers;
    }
}

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
import org.apache.lucene.util.BytesRefBuilder;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static java.lang.Math.max;
import static org.hamcrest.Matchers.equalTo;

public class MvZipTests extends AbstractScalarFunctionTestCase {
    public MvZipTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        // Note that any null is *not* null, so we explicitly test with nulls
        List<TestCaseSupplier> suppliers = new ArrayList<>();
        for (DataType leftType : DataType.types()) {
            if (leftType != DataType.NULL && DataType.isString(leftType) == false) {
                continue;
            }
            for (DataType rightType : DataType.types()) {
                if (rightType != DataType.NULL && DataType.isString(rightType) == false) {
                    continue;
                }
                for (DataType delimType : DataType.types()) {
                    if (delimType != DataType.NULL && DataType.isString(delimType) == false) {
                        continue;
                    }
                    suppliers.add(supplier(leftType, rightType, delimType));
                }
                suppliers.add(supplier(leftType, rightType));
            }
        }

        return parameterSuppliersFromTypedData(suppliers);
    }

    private static TestCaseSupplier supplier(DataType leftType, DataType rightType, DataType delimType) {
        return new TestCaseSupplier(List.of(leftType, rightType, delimType), () -> {
            List<BytesRef> left = randomList(leftType);
            List<BytesRef> right = randomList(rightType);
            BytesRef delim = delimType == DataType.NULL ? null : new BytesRef(randomAlphaOfLength(1));

            List<BytesRef> expected = calculateExpected(left, right, delim);
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(left, leftType, "mvLeft"),
                    new TestCaseSupplier.TypedData(right, rightType, "mvRight"),
                    new TestCaseSupplier.TypedData(delim, delimType, "delim")
                ),
                "MvZipEvaluator[leftField=Attribute[channel=0], rightField=Attribute[channel=1], delim=Attribute[channel=2]]",
                DataType.KEYWORD,
                equalTo(expected == null ? null : expected.size() == 1 ? expected.iterator().next() : expected)
            );
        });
    }

    private static TestCaseSupplier supplier(DataType leftType, DataType rightType) {
        return new TestCaseSupplier(List.of(leftType, rightType), () -> {
            List<BytesRef> left = randomList(leftType);
            List<BytesRef> right = randomList(rightType);

            List<BytesRef> expected = calculateExpected(left, right, new BytesRef(","));
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(left, leftType, "mvLeft"),
                    new TestCaseSupplier.TypedData(right, rightType, "mvRight")
                ),
                "MvZipEvaluator[leftField=Attribute[channel=0], rightField=Attribute[channel=1], delim=LiteralsEvaluator[lit=,]]",
                DataType.KEYWORD,
                equalTo(expected == null ? null : expected.size() == 1 ? expected.iterator().next() : expected)
            );
        });
    }

    private static List<BytesRef> randomList(DataType type) {
        if (type == DataType.NULL) {
            return null;
        }
        return randomList(1, 3, () -> new BytesRef(randomAlphaOfLength(5)));
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new MvZip(source, args.get(0), args.get(1), args.size() > 2 ? args.get(2) : null);
    }

    private static List<BytesRef> calculateExpected(List<BytesRef> left, List<BytesRef> right, BytesRef delim) {
        if (delim == null) {
            return null;
        }
        if (left == null) {
            return right;
        }
        if (right == null) {
            return left;
        }
        List<BytesRef> expected = new ArrayList<>(max(left.size(), right.size()));
        int i = 0, j = 0;
        while (i < left.size() && j < right.size()) {
            BytesRefBuilder work = new BytesRefBuilder();
            work.append(left.get(i));
            work.append(delim);
            work.append(right.get(j));
            expected.add(work.get());
            i++;
            j++;
        }
        while (i < left.size()) {
            BytesRefBuilder work = new BytesRefBuilder();
            work.append(left.get(i));
            expected.add(work.get());
            i++;
        }
        while (j < right.size()) {
            BytesRefBuilder work = new BytesRefBuilder();
            work.append(right.get(j));
            expected.add(work.get());
            j++;
        }
        return expected;
    }
}

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
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.elasticsearch.xpack.esql.expression.function.scalar.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;

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
        List<TestCaseSupplier> suppliers = new ArrayList<>();
        suppliers.add(new TestCaseSupplier(List.of(DataTypes.KEYWORD, DataTypes.KEYWORD, DataTypes.KEYWORD), () -> {
            List<Object> left = randomList(1, 3, () -> randomLiteral(DataTypes.KEYWORD).value());
            List<Object> right = randomList(1, 3, () -> randomLiteral(DataTypes.KEYWORD).value());
            String delim = randomAlphaOfLengthBetween(1, 1);
            List<BytesRef> expected = calculateExpected(left, right, delim);
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(left, DataTypes.KEYWORD, "mvLeft"),
                    new TestCaseSupplier.TypedData(right, DataTypes.KEYWORD, "mvRight"),
                    new TestCaseSupplier.TypedData(delim, DataTypes.KEYWORD, "delim")
                ),
                "MvZipEvaluator[leftField=Attribute[channel=0], rightField=Attribute[channel=1], delim=Attribute[channel=2]]",
                DataTypes.KEYWORD,
                equalTo(expected.size() == 1 ? expected.iterator().next() : expected)
            );
        }));

        suppliers.add(new TestCaseSupplier(List.of(DataTypes.TEXT, DataTypes.TEXT, DataTypes.TEXT), () -> {
            List<Object> left = randomList(1, 10, () -> randomLiteral(DataTypes.TEXT).value());
            List<Object> right = randomList(1, 10, () -> randomLiteral(DataTypes.TEXT).value());
            String delim = randomAlphaOfLengthBetween(1, 1);
            List<BytesRef> expected = calculateExpected(left, right, delim);
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(left, DataTypes.TEXT, "mvLeft"),
                    new TestCaseSupplier.TypedData(right, DataTypes.TEXT, "mvRight"),
                    new TestCaseSupplier.TypedData(delim, DataTypes.TEXT, "delim")
                ),
                "MvZipEvaluator[leftField=Attribute[channel=0], rightField=Attribute[channel=1], delim=Attribute[channel=2]]",
                DataTypes.KEYWORD,
                equalTo(expected.size() == 1 ? expected.iterator().next() : expected)
            );
        }));

        return parameterSuppliersFromTypedData(suppliers);
    }

    @Override
    protected DataType expectedType(List<DataType> argTypes) {
        return DataTypes.KEYWORD;
    }

    @Override
    protected List<ArgumentSpec> argSpec() {
        return List.of(required(strings()), required(strings()), optional(strings()));
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new MvZip(source, args.get(0), args.get(1), args.size() > 2 ? args.get(2) : null);
    }

    private static List<BytesRef> calculateExpected(List<Object> left, List<Object> right, String delim) {
        List<BytesRef> expected = new ArrayList<>(max(left.size(), right.size()));
        int i = 0, j = 0;
        while (i < left.size() && j < right.size()) {
            BytesRefBuilder work = new BytesRefBuilder();
            work.append((BytesRef) left.get(i));
            work.append(new BytesRef(delim));
            work.append((BytesRef) right.get(j));
            expected.add(work.get());
            i++;
            j++;
        }
        while (i < left.size()) {
            BytesRefBuilder work = new BytesRefBuilder();
            work.append((BytesRef) left.get(i));
            expected.add(work.get());
            i++;
        }
        while (j < right.size()) {
            BytesRefBuilder work = new BytesRefBuilder();
            work.append((BytesRef) right.get(j));
            expected.add(work.get());
            j++;
        }
        return expected;
    }

    @Override
    public void testSimpleWithNulls() {
        assumeFalse("mv_zip returns null only if both left and right inputs are nulls", false);
    }
}

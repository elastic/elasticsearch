/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.multivalue;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;

public class MvMedianTests extends AbstractMultivalueFunctionTestCase {
    public MvMedianTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> cases = new ArrayList<>();
        doubles(cases, "mv_median", "MvMedian", (size, values) -> {
            int middle = size / 2;
            if (size % 2 == 1) {
                return equalTo(values.sorted().skip(middle).findFirst().getAsDouble());
            }
            return equalTo(values.sorted().skip(middle - 1).limit(2).average().getAsDouble());
        });
        ints(cases, "mv_median", "MvMedian", (size, values) -> {
            int middle = size / 2;
            if (size % 2 == 1) {
                return equalTo(values.sorted().skip(middle).findFirst().getAsInt());
            }
            var s = values.sorted().skip(middle - 1).limit(2).iterator();
            BigInteger a = BigInteger.valueOf(s.next());
            BigInteger b = BigInteger.valueOf(s.next());
            return equalTo(a.add(b.subtract(a).divide(BigInteger.valueOf(2))).intValue());
        });
        longs(cases, "mv_median", "MvMedian", (size, values) -> {
            int middle = size / 2;
            if (size % 2 == 1) {
                return equalTo(values.sorted().skip(middle).findFirst().getAsLong());
            }
            var s = values.sorted().skip(middle - 1).limit(2).iterator();
            BigInteger a = BigInteger.valueOf(s.next());
            BigInteger b = BigInteger.valueOf(s.next());
            return equalTo(a.add(b.subtract(a).divide(BigInteger.valueOf(2))).longValue());
        });
        unsignedLongs(cases, "mv_median", "MvMedian", (size, values) -> {
            int middle = size / 2;
            if (size % 2 == 1) {
                return equalTo(values.sorted().skip(middle).findFirst().get());
            }
            var s = values.sorted().skip(middle - 1).limit(2).iterator();
            BigInteger a = s.next();
            BigInteger b = s.next();
            return equalTo(a.add(b.subtract(a).divide(BigInteger.valueOf(2))));
        });

        cases.add(
            new TestCaseSupplier(
                "mv_median(<1, 2>)",
                List.of(DataType.INTEGER),
                () -> new TestCaseSupplier.TestCase(
                    List.of(new TestCaseSupplier.TypedData(List.of(1, 2), DataType.INTEGER, "field")),
                    "MvMedian[field=Attribute[channel=0]]",
                    DataType.INTEGER,
                    equalTo(1)
                )
            )
        );
        cases.add(
            new TestCaseSupplier(
                "mv_median(<-1, -2>)",
                List.of(DataType.INTEGER),
                () -> new TestCaseSupplier.TestCase(
                    List.of(new TestCaseSupplier.TypedData(List.of(-1, -2), DataType.INTEGER, "field")),
                    "MvMedian[field=Attribute[channel=0]]",
                    DataType.INTEGER,
                    equalTo(-2)
                )
            )
        );
        return parameterSuppliersFromTypedDataWithDefaultChecksNoErrors(false, cases);
    }

    @Override
    protected Expression build(Source source, Expression field) {
        return new MvMedian(source, field);
    }
}

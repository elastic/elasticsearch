/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.conditional;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.elasticsearch.xpack.esql.expression.function.scalar.VaragsTestCaseBuilder;

import java.util.Comparator;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static org.hamcrest.Matchers.equalTo;

public class GreatestTests extends AbstractScalarFunctionTestCase {
    public GreatestTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        VaragsTestCaseBuilder builder = new VaragsTestCaseBuilder(t -> "Greatest" + t);
        builder.expectedEvaluatorValueWrap(e -> "MvMax[field=" + e + "]");
        builder.expectFlattenedString(s -> s.sorted(Comparator.<String>naturalOrder().reversed()).findFirst());
        builder.expectFlattenedBoolean(s -> s.sorted(Comparator.<Boolean>naturalOrder().reversed()).findFirst());
        builder.expectFlattenedInt(IntStream::max);
        builder.expectFlattenedLong(LongStream::max);
        List<TestCaseSupplier> suppliers = builder.suppliers();
        suppliers.add(
            new TestCaseSupplier(
                "(a, b)",
                List.of(DataType.KEYWORD, DataType.KEYWORD),
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(new BytesRef("a"), DataType.KEYWORD, "a"),
                        new TestCaseSupplier.TypedData(new BytesRef("b"), DataType.KEYWORD, "b")
                    ),
                    "GreatestBytesRefEvaluator[values=[MvMax[field=Attribute[channel=0]], MvMax[field=Attribute[channel=1]]]]",
                    DataType.KEYWORD,
                    equalTo(new BytesRef("b"))
                )
            )
        );
        suppliers.add(
            new TestCaseSupplier(
                "(a, b)",
                List.of(DataType.VERSION, DataType.VERSION),
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(new BytesRef("1"), DataType.VERSION, "a"),
                        new TestCaseSupplier.TypedData(new BytesRef("2"), DataType.VERSION, "b")
                    ),
                    "GreatestBytesRefEvaluator[values=[MvMax[field=Attribute[channel=0]], MvMax[field=Attribute[channel=1]]]]",
                    DataType.VERSION,
                    equalTo(new BytesRef("2"))
                )
            )
        );
        suppliers.add(
            new TestCaseSupplier(
                "(a, b)",
                List.of(DataType.IP, DataType.IP),
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(new BytesRef("127.0.0.1"), DataType.IP, "a"),
                        new TestCaseSupplier.TypedData(new BytesRef("127.0.0.2"), DataType.IP, "b")
                    ),
                    "GreatestBytesRefEvaluator[values=[MvMax[field=Attribute[channel=0]], MvMax[field=Attribute[channel=1]]]]",
                    DataType.IP,
                    equalTo(new BytesRef("127.0.0.2"))
                )
            )
        );
        suppliers.add(
            new TestCaseSupplier(
                "(a, b)",
                List.of(DataType.DOUBLE, DataType.DOUBLE),
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(1d, DataType.DOUBLE, "a"),
                        new TestCaseSupplier.TypedData(2d, DataType.DOUBLE, "b")
                    ),
                    "GreatestDoubleEvaluator[values=[MvMax[field=Attribute[channel=0]], MvMax[field=Attribute[channel=1]]]]",
                    DataType.DOUBLE,
                    equalTo(2d)
                )
            )
        );
        suppliers.add(
            new TestCaseSupplier(
                "(a, b)",
                List.of(DataType.DATETIME, DataType.DATETIME),
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(1727877348000L, DataType.DATETIME, "a"),
                        new TestCaseSupplier.TypedData(1727790948000L, DataType.DATETIME, "b")
                    ),
                    "GreatestLongEvaluator[values=[MvMax[field=Attribute[channel=0]], MvMax[field=Attribute[channel=1]]]]",
                    DataType.DATETIME,
                    equalTo(1727877348000L)
                )
            )
        );
        suppliers.add(
            new TestCaseSupplier(
                "(a, b)",
                List.of(DataType.DATE_NANOS, DataType.DATE_NANOS),
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(1727877348000123456L, DataType.DATE_NANOS, "a"),
                        new TestCaseSupplier.TypedData(1727790948000987654L, DataType.DATE_NANOS, "b")
                    ),
                    "GreatestLongEvaluator[values=[MvMax[field=Attribute[channel=0]], MvMax[field=Attribute[channel=1]]]]",
                    DataType.DATE_NANOS,
                    equalTo(1727877348000123456L)
                )
            )
        );
        return parameterSuppliersFromTypedData(anyNullIsNull(false, suppliers));
    }

    @Override
    protected Greatest build(Source source, List<Expression> args) {
        return new Greatest(source, args.get(0), args.subList(1, args.size()));
    }
}

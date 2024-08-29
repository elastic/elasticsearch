/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.grouping;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Rounding;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.hamcrest.Matcher;

import java.time.Duration;
import java.time.Period;
import java.util.ArrayList;
import java.util.List;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;

public class BucketTests extends AbstractScalarFunctionTestCase {
    public BucketTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();
        dateCases(suppliers, "fixed date", () -> DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis("2023-02-17T09:00:00.00Z"));
        dateCasesWithSpan(
            suppliers,
            "fixed date with period",
            () -> DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis("2023-01-01T00:00:00.00Z"),
            DataType.DATE_PERIOD,
            Period.ofYears(1),
            "[YEAR_OF_CENTURY in Z][fixed to midnight]"
        );
        dateCasesWithSpan(
            suppliers,
            "fixed date with duration",
            () -> DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis("2023-02-17T09:00:00.00Z"),
            DataType.TIME_DURATION,
            Duration.ofDays(1L),
            "[86400000 in Z][fixed]"
        );
        numberCases(suppliers, "fixed long", DataType.LONG, () -> 100L);
        numberCasesWithSpan(suppliers, "fixed long with span", DataType.LONG, () -> 100L);
        numberCases(suppliers, "fixed int", DataType.INTEGER, () -> 100);
        numberCasesWithSpan(suppliers, "fixed int with span", DataType.INTEGER, () -> 100);
        numberCases(suppliers, "fixed double", DataType.DOUBLE, () -> 100.0);
        numberCasesWithSpan(suppliers, "fixed double with span", DataType.DOUBLE, () -> 100.);
        // TODO make errorsForCasesWithoutExamples do something sensible for 4+ parameters
        return parameterSuppliersFromTypedData(
            anyNullIsNull(
                suppliers,
                (nullPosition, nullValueDataType, original) -> nullPosition == 0 && nullValueDataType == DataType.NULL
                    ? DataType.NULL
                    : original.expectedType(),
                (nullPosition, nullData, original) -> nullPosition == 0 ? original : equalTo("LiteralsEvaluator[lit=null]")
            )
        );
    }

    // TODO once we cast above the functions we can drop these
    private static final DataType[] DATE_BOUNDS_TYPE = new DataType[] { DataType.DATETIME, DataType.KEYWORD, DataType.TEXT };

    private static void dateCases(List<TestCaseSupplier> suppliers, String name, LongSupplier date) {
        for (DataType fromType : DATE_BOUNDS_TYPE) {
            for (DataType toType : DATE_BOUNDS_TYPE) {
                suppliers.add(new TestCaseSupplier(name, List.of(DataType.DATETIME, DataType.INTEGER, fromType, toType), () -> {
                    List<TestCaseSupplier.TypedData> args = new ArrayList<>();
                    args.add(new TestCaseSupplier.TypedData(date.getAsLong(), DataType.DATETIME, "field"));
                    // TODO more "from" and "to" and "buckets"
                    args.add(new TestCaseSupplier.TypedData(50, DataType.INTEGER, "buckets").forceLiteral());
                    args.add(dateBound("from", fromType, "2023-02-01T00:00:00.00Z"));
                    args.add(dateBound("to", toType, "2023-03-01T09:00:00.00Z"));
                    return new TestCaseSupplier.TestCase(
                        args,
                        "DateTruncEvaluator[fieldVal=Attribute[channel=0], rounding=Rounding[DAY_OF_MONTH in Z][fixed to midnight]]",
                        DataType.DATETIME,
                        resultsMatcher(args)
                    );
                }));
                // same as above, but a low bucket count and datetime bounds that match it (at hour span)
                suppliers.add(new TestCaseSupplier(name, List.of(DataType.DATETIME, DataType.INTEGER, fromType, toType), () -> {
                    List<TestCaseSupplier.TypedData> args = new ArrayList<>();
                    args.add(new TestCaseSupplier.TypedData(date.getAsLong(), DataType.DATETIME, "field"));
                    args.add(new TestCaseSupplier.TypedData(4, DataType.INTEGER, "buckets").forceLiteral());
                    args.add(dateBound("from", fromType, "2023-02-17T09:00:00Z"));
                    args.add(dateBound("to", toType, "2023-02-17T12:00:00Z"));
                    return new TestCaseSupplier.TestCase(
                        args,
                        "DateTruncEvaluator[fieldVal=Attribute[channel=0], rounding=Rounding[3600000 in Z][fixed]]",
                        DataType.DATETIME,
                        equalTo(Rounding.builder(Rounding.DateTimeUnit.HOUR_OF_DAY).build().prepareForUnknown().round(date.getAsLong()))
                    );
                }));
            }
        }
    }

    private static TestCaseSupplier.TypedData dateBound(String name, DataType type, String date) {
        Object value;
        if (type == DataType.DATETIME) {
            value = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis(date);
        } else {
            value = new BytesRef(date);
        }
        return new TestCaseSupplier.TypedData(value, type, name).forceLiteral();
    }

    private static void dateCasesWithSpan(
        List<TestCaseSupplier> suppliers,
        String name,
        LongSupplier date,
        DataType spanType,
        Object span,
        String spanStr
    ) {
        suppliers.add(new TestCaseSupplier(name, List.of(DataType.DATETIME, spanType), () -> {
            List<TestCaseSupplier.TypedData> args = new ArrayList<>();
            args.add(new TestCaseSupplier.TypedData(date.getAsLong(), DataType.DATETIME, "field"));
            args.add(new TestCaseSupplier.TypedData(span, spanType, "buckets").forceLiteral());
            return new TestCaseSupplier.TestCase(
                args,
                "DateTruncEvaluator[fieldVal=Attribute[channel=0], rounding=Rounding" + spanStr + "]",
                DataType.DATETIME,
                resultsMatcher(args)
            );
        }));
    }

    private static final DataType[] NUMBER_BOUNDS_TYPES = new DataType[] { DataType.INTEGER, DataType.LONG, DataType.DOUBLE };

    private static void numberCases(List<TestCaseSupplier> suppliers, String name, DataType numberType, Supplier<Number> number) {
        for (DataType fromType : NUMBER_BOUNDS_TYPES) {
            for (DataType toType : NUMBER_BOUNDS_TYPES) {
                suppliers.add(new TestCaseSupplier(name, List.of(numberType, DataType.INTEGER, fromType, toType), () -> {
                    List<TestCaseSupplier.TypedData> args = new ArrayList<>();
                    args.add(new TestCaseSupplier.TypedData(number.get(), "field"));
                    // TODO more "from" and "to" and "buckets"
                    args.add(new TestCaseSupplier.TypedData(50, DataType.INTEGER, "buckets").forceLiteral());
                    args.add(numericBound("from", fromType, 0.0));
                    args.add(numericBound("to", toType, 1000.0));
                    // TODO more number types for "from" and "to"
                    String attr = "Attribute[channel=0]";
                    if (numberType == DataType.INTEGER) {
                        attr = "CastIntToDoubleEvaluator[v=" + attr + "]";
                    } else if (numberType == DataType.LONG) {
                        attr = "CastLongToDoubleEvaluator[v=" + attr + "]";
                    }
                    return new TestCaseSupplier.TestCase(
                        args,
                        "MulDoublesEvaluator[lhs=FloorDoubleEvaluator[val=DivDoublesEvaluator[lhs="
                            + attr
                            + ", "
                            + "rhs=LiteralsEvaluator[lit=50.0]]], rhs=LiteralsEvaluator[lit=50.0]]",
                        DataType.DOUBLE,
                        resultsMatcher(args)
                    );
                }));
            }
        }
    }

    private static TestCaseSupplier.TypedData numericBound(String name, DataType type, double value) {
        Number v;
        if (type == DataType.INTEGER) {
            v = (int) value;
        } else if (type == DataType.LONG) {
            v = (long) value;
        } else {
            v = value;
        }
        return new TestCaseSupplier.TypedData(v, type, name).forceLiteral();
    }

    private static void numberCasesWithSpan(List<TestCaseSupplier> suppliers, String name, DataType numberType, Supplier<Number> number) {
        for (Number span : List.of(50, 50L, 50d)) {
            DataType spanType = DataType.fromJava(span);
            suppliers.add(new TestCaseSupplier(name, List.of(numberType, spanType), () -> {
                List<TestCaseSupplier.TypedData> args = new ArrayList<>();
                args.add(new TestCaseSupplier.TypedData(number.get(), "field"));
                args.add(new TestCaseSupplier.TypedData(span, spanType, "span").forceLiteral());
                String attr = "Attribute[channel=0]";
                if (numberType == DataType.INTEGER) {
                    attr = "CastIntToDoubleEvaluator[v=" + attr + "]";
                } else if (numberType == DataType.LONG) {
                    attr = "CastLongToDoubleEvaluator[v=" + attr + "]";
                }
                return new TestCaseSupplier.TestCase(
                    args,
                    "MulDoublesEvaluator[lhs=FloorDoubleEvaluator[val=DivDoublesEvaluator[lhs="
                        + attr
                        + ", "
                        + "rhs=LiteralsEvaluator[lit=50.0]]], rhs=LiteralsEvaluator[lit=50.0]]",
                    DataType.DOUBLE,
                    resultsMatcher(args)
                );
            }));
        }

    }

    private static TestCaseSupplier.TypedData keywordDateLiteral(String name, DataType type, String date) {
        return new TestCaseSupplier.TypedData(date, type, name).forceLiteral();
    }

    private static Matcher<Object> resultsMatcher(List<TestCaseSupplier.TypedData> typedData) {
        if (typedData.get(0).type() == DataType.DATETIME) {
            long millis = ((Number) typedData.get(0).data()).longValue();
            return equalTo(Rounding.builder(Rounding.DateTimeUnit.DAY_OF_MONTH).build().prepareForUnknown().round(millis));
        }
        return equalTo(((Number) typedData.get(0).data()).doubleValue());
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        Expression from = null;
        Expression to = null;
        if (args.size() > 2) {
            from = args.get(2);
            to = args.get(3);
        }
        return new Bucket(source, args.get(0), args.get(1), from, to);
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.math;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Rounding;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.xpack.esql.expression.function.AbstractFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.elasticsearch.xpack.esql.expression.function.grouping.Bucket;
import org.elasticsearch.xpack.esql.type.EsqlDataTypes;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.hamcrest.Matcher;

import java.time.Duration;
import java.time.Period;
import java.util.ArrayList;
import java.util.List;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;

public class BucketTests extends AbstractFunctionTestCase {
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
            EsqlDataTypes.DATE_PERIOD,
            Period.ofYears(1),
            "[YEAR_OF_CENTURY in Z][fixed to midnight]"
        );
        dateCasesWithSpan(
            suppliers,
            "fixed date with duration",
            () -> DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis("2023-02-17T09:00:00.00Z"),
            EsqlDataTypes.TIME_DURATION,
            Duration.ofDays(1L),
            "[86400000 in Z][fixed]"
        );
        numberCases(suppliers, "fixed long", DataTypes.LONG, () -> 100L);
        numberCasesWithSpan(suppliers, "fixed long with span", DataTypes.LONG, () -> 100L);
        numberCases(suppliers, "fixed int", DataTypes.INTEGER, () -> 100);
        numberCasesWithSpan(suppliers, "fixed int with span", DataTypes.INTEGER, () -> 100);
        numberCases(suppliers, "fixed double", DataTypes.DOUBLE, () -> 100.0);
        numberCasesWithSpan(suppliers, "fixed double with span", DataTypes.DOUBLE, () -> 100.);
        // TODO make errorsForCasesWithoutExamples do something sensible for 4+ parameters
        return parameterSuppliersFromTypedData(
            anyNullIsNull(
                suppliers,
                (nullPosition, nullValueDataType, original) -> nullPosition == 0 && nullValueDataType == DataTypes.NULL
                    ? DataTypes.NULL
                    : original.expectedType(),
                (nullPosition, nullData, original) -> nullPosition == 0 ? original : equalTo("LiteralsEvaluator[lit=null]")
            )
        );
    }

    // TODO once we cast above the functions we can drop these
    private static final DataType[] DATE_BOUNDS_TYPE = new DataType[] { DataTypes.DATETIME };

    private static void dateCases(List<TestCaseSupplier> suppliers, String name, LongSupplier date) {
        for (DataType fromType : DATE_BOUNDS_TYPE) {
            for (DataType toType : DATE_BOUNDS_TYPE) {
                suppliers.add(new TestCaseSupplier(name, List.of(DataTypes.DATETIME, DataTypes.INTEGER, fromType, toType), () -> {
                    List<TestCaseSupplier.TypedData> args = new ArrayList<>();
                    args.add(new TestCaseSupplier.TypedData(date.getAsLong(), DataTypes.DATETIME, "field"));
                    // TODO more "from" and "to" and "buckets"
                    args.add(new TestCaseSupplier.TypedData(50, DataTypes.INTEGER, "buckets").forceLiteral());
                    args.add(dateBound("from", fromType, "2023-02-01T00:00:00.00Z"));
                    args.add(dateBound("to", toType, "2023-03-01T09:00:00.00Z"));
                    return new TestCaseSupplier.TestCase(
                        args,
                        "DateTruncEvaluator[fieldVal=Attribute[channel=0], rounding=Rounding[DAY_OF_MONTH in Z][fixed to midnight]]",
                        DataTypes.DATETIME,
                        dateResultsMatcher(args)
                    );
                }));
            }
        }
    }

    private static TestCaseSupplier.TypedData dateBound(String name, DataType type, String date) {
        Object value;
        if (type == DataTypes.DATETIME) {
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
        suppliers.add(new TestCaseSupplier(name, List.of(DataTypes.DATETIME, spanType), () -> {
            List<TestCaseSupplier.TypedData> args = new ArrayList<>();
            args.add(new TestCaseSupplier.TypedData(date.getAsLong(), DataTypes.DATETIME, "field"));
            args.add(new TestCaseSupplier.TypedData(span, spanType, "buckets").forceLiteral());
            return new TestCaseSupplier.TestCase(
                args,
                "DateTruncEvaluator[fieldVal=Attribute[channel=0], rounding=Rounding" + spanStr + "]",
                DataTypes.DATETIME,
                dateResultsMatcher(args)
            );
        }));
    }

    private static final DataType[] NUMBER_BOUNDS_TYPES = new DataType[] { DataTypes.INTEGER, DataTypes.LONG, DataTypes.DOUBLE };

    private static void numberCases(List<TestCaseSupplier> suppliers, String name, DataType numberType, Supplier<Number> number) {
        for (DataType fromType : NUMBER_BOUNDS_TYPES) {
            for (DataType toType : NUMBER_BOUNDS_TYPES) {
                suppliers.add(new TestCaseSupplier(name, List.of(numberType, DataTypes.INTEGER, fromType, toType), () -> {
                    List<TestCaseSupplier.TypedData> args = new ArrayList<>();
                    args.add(new TestCaseSupplier.TypedData(number.get(), "field"));
                    // TODO more "from" and "to" and "buckets"
                    args.add(new TestCaseSupplier.TypedData(50, DataTypes.INTEGER, "buckets").forceLiteral());
                    args.add(numericBound("from", fromType, 0.0));
                    args.add(numericBound("to", toType, 1000.0));
                    // TODO more number types for "from" and "to"
                    String attr = "Attribute[channel=0]";
                    if (numberType == DataTypes.INTEGER) {
                        attr = "CastIntToDoubleEvaluator[v=" + attr + "]";
                    } else if (numberType == DataTypes.LONG) {
                        attr = "CastLongToDoubleEvaluator[v=" + attr + "]";
                    }
                    return new TestCaseSupplier.TestCase(
                        args,
                        "MulDoublesEvaluator[lhs=FloorDoubleEvaluator[val=DivDoublesEvaluator[lhs="
                            + attr
                            + ", "
                            + "rhs=LiteralsEvaluator[lit=50.0]]], rhs=LiteralsEvaluator[lit=50.0]]",
                        DataTypes.DOUBLE,
                        dateResultsMatcher(args)
                    );
                }));
            }
        }
    }

    private static TestCaseSupplier.TypedData numericBound(String name, DataType type, double value) {
        Number v;
        if (type == DataTypes.INTEGER) {
            v = (int) value;
        } else if (type == DataTypes.LONG) {
            v = (long) value;
        } else {
            v = value;
        }
        return new TestCaseSupplier.TypedData(v, type, name).forceLiteral();
    }

    private static void numberCasesWithSpan(List<TestCaseSupplier> suppliers, String name, DataType numberType, Supplier<Number> number) {
        suppliers.add(new TestCaseSupplier(name, List.of(numberType, DataTypes.DOUBLE), () -> {
            List<TestCaseSupplier.TypedData> args = new ArrayList<>();
            args.add(new TestCaseSupplier.TypedData(number.get(), "field"));
            args.add(new TestCaseSupplier.TypedData(50., DataTypes.DOUBLE, "span").forceLiteral());
            String attr = "Attribute[channel=0]";
            if (numberType == DataTypes.INTEGER) {
                attr = "CastIntToDoubleEvaluator[v=" + attr + "]";
            } else if (numberType == DataTypes.LONG) {
                attr = "CastLongToDoubleEvaluator[v=" + attr + "]";
            }
            return new TestCaseSupplier.TestCase(
                args,
                "MulDoublesEvaluator[lhs=FloorDoubleEvaluator[val=DivDoublesEvaluator[lhs="
                    + attr
                    + ", "
                    + "rhs=LiteralsEvaluator[lit=50.0]]], rhs=LiteralsEvaluator[lit=50.0]]",
                DataTypes.DOUBLE,
                dateResultsMatcher(args)
            );
        }));

    }

    private static TestCaseSupplier.TypedData keywordDateLiteral(String name, DataType type, String date) {
        return new TestCaseSupplier.TypedData(date, type, name).forceLiteral();
    }

    private static Matcher<Object> dateResultsMatcher(List<TestCaseSupplier.TypedData> typedData) {
        if (typedData.get(0).type() == DataTypes.DATETIME) {
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

    @Override
    public void testSimpleWithNulls() {
        assumeFalse("we test nulls in parameters", true);
    }
}

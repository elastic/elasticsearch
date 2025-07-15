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
import org.elasticsearch.common.time.DateUtils;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.hamcrest.Matcher;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;

public class EmptyBucketTests extends AbstractScalarFunctionTestCase {

    public EmptyBucketTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();
        dateCase(suppliers, "fixed date");
        return parameterSuppliersFromTypedData(suppliers);
    }

    private static void dateCase(List<TestCaseSupplier> suppliers, String name) {
        DataType fromType = DataType.DATETIME;
        DataType toType = DataType.DATETIME;
        long date = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis("2023-02-17T09:00:00.00Z");
        suppliers.add(new TestCaseSupplier(name, List.of(DataType.DATETIME, DataType.INTEGER, fromType, toType), () -> {
            List<TestCaseSupplier.TypedData> args = new ArrayList<>();
            args.add(new TestCaseSupplier.TypedData(date, DataType.DATETIME, "field"));
            // TODO more "from" and "to" and "buckets"
            args.add(new TestCaseSupplier.TypedData(50, DataType.INTEGER, "buckets").forceLiteral());
            args.add(dateBound("from", fromType, "2023-02-01T00:00:00.00Z"));
            args.add(dateBound("to", toType, "2023-03-01T09:00:00.00Z"));
            return new TestCaseSupplier.TestCase(
                args,
                "DateTruncDatetimeEvaluator[fieldVal=Attribute[channel=0], " + "rounding=Rounding[DAY_OF_MONTH in Z][fixed to midnight]]",
                DataType.DATETIME,
                resultsMatcher(args)
            );
        }));
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

    private static Matcher<Object> resultsMatcher(List<TestCaseSupplier.TypedData> typedData) {
        if (typedData.get(0).type() == DataType.DATETIME) {
            long millis = ((Number) typedData.get(0).data()).longValue();
            long expected = Rounding.builder(Rounding.DateTimeUnit.DAY_OF_MONTH).build().prepareForUnknown().round(millis);
            LogManager.getLogger(getTestClass()).info("Expected: " + Instant.ofEpochMilli(expected));
            LogManager.getLogger(getTestClass()).info("Input: " + Instant.ofEpochMilli(millis));
            return equalTo(expected);
        }
        if (typedData.get(0).type() == DataType.DATE_NANOS) {
            long nanos = ((Number) typedData.get(0).data()).longValue();
            long expected = DateUtils.toNanoSeconds(
                Rounding.builder(Rounding.DateTimeUnit.DAY_OF_MONTH).build().prepareForUnknown().round(DateUtils.toMilliSeconds(nanos))
            );
            LogManager.getLogger(getTestClass()).info("Expected: " + DateUtils.toInstant(expected));
            LogManager.getLogger(getTestClass()).info("Input: " + DateUtils.toInstant(nanos));
            return equalTo(expected);
        }
        return equalTo(((Number) typedData.get(0).data()).doubleValue());
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        Expression from = null;
        Expression to = null;
        Expression emitEmptyBuckets = null;
        if (args.size() > 2) {
            from = args.get(2);
            to = args.get(3);
        }
        if (args.size() > 4) {
            emitEmptyBuckets = args.get(4);
        }
        return new Bucket(source, args.get(0), args.get(1), from, to, emitEmptyBuckets);
    }
}

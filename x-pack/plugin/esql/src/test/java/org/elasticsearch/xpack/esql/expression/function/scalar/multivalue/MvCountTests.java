/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.multivalue;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.compute.data.LongRangeBlockBuilder;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesTo;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesToLifecycle;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.test.ESTestCase.randomList;
import static org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier.appliesTo;
import static org.hamcrest.Matchers.equalTo;

public class MvCountTests extends AbstractMultivalueFunctionTestCase {
    public MvCountTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> cases = new ArrayList<>();
        booleans(cases, "mv_count", "MvCount", DataType.INTEGER, (size, values) -> equalTo(Math.toIntExact(values.count())));
        bytesRefs(cases, "mv_count", "MvCount", t -> DataType.INTEGER, (size, values) -> equalTo(Math.toIntExact(values.count())));
        doubles(cases, "mv_count", "MvCount", DataType.INTEGER, (size, values) -> equalTo(Math.toIntExact(values.count())));
        flattened(cases, "mv_count", "MvCount", t -> DataType.INTEGER, (size, values) -> equalTo(size));
        ints(cases, "mv_count", "MvCount", DataType.INTEGER, (size, values) -> equalTo(Math.toIntExact(values.count())));
        longs(cases, "mv_count", "MvCount", DataType.INTEGER, (size, values) -> equalTo(Math.toIntExact(values.count())));
        unsignedLongs(cases, "mv_count", "MvCount", DataType.INTEGER, (size, values) -> equalTo(Math.toIntExact(values.count())));
        dateTimes(cases, "mv_count", "MvCount", DataType.INTEGER, (size, values) -> equalTo(Math.toIntExact(values.count())));
        dateNanos(cases, "mv_count", "MvCount", DataType.INTEGER, (size, values) -> equalTo(Math.toIntExact(values.count())));
        dateRanges(cases);
        geoPoints(cases, "mv_count", "MvCount", DataType.INTEGER, (size, values) -> equalTo(Math.toIntExact(values.count())));
        cartesianPoints(cases, "mv_count", "MvCount", DataType.INTEGER, (size, values) -> equalTo(Math.toIntExact(values.count())));
        geoShape(cases, "mv_count", "MvCount", DataType.INTEGER, (size, values) -> equalTo(Math.toIntExact(values.count())));
        cartesianShape(cases, "mv_count", "MvCount", DataType.INTEGER, (size, values) -> equalTo(Math.toIntExact(values.count())));
        geohashGrid(cases, "mv_count", "MvCount", DataType.INTEGER, (size, values) -> equalTo(Math.toIntExact(values.count())));
        geotileGrid(cases, "mv_count", "MvCount", DataType.INTEGER, (size, values) -> equalTo(Math.toIntExact(values.count())));
        geohexGrid(cases, "mv_count", "MvCount", DataType.INTEGER, (size, values) -> equalTo(Math.toIntExact(values.count())));
        return parameterSuppliersFromTypedDataWithDefaultChecks(true, cases);
    }

    @Override
    protected Expression build(Source source, Expression field) {
        return new MvCount(source, field);
    }

    @Override
    protected DataType expectedType(List<DataType> argTypes) {
        return DataType.INTEGER;
    }

    private static void dateRanges(List<TestCaseSupplier> cases) {
        FunctionAppliesTo dateRangeAppliesTo = appliesTo(FunctionAppliesToLifecycle.PREVIEW, "9.5.0", "", false);
        cases.add(new TestCaseSupplier("mv_count(date_range)", List.of(DataType.DATE_RANGE), () -> {
            LongRangeBlockBuilder.LongRange value = TestCaseSupplier.randomDateRange();
            return new TestCaseSupplier.TestCase(
                List.of(new TestCaseSupplier.TypedData(List.of(value), DataType.DATE_RANGE, "field").withAppliesTo(dateRangeAppliesTo)),
                "MvCount[field=Attribute[channel=0]]",
                DataType.INTEGER,
                equalTo(1)
            );
        }));
        cases.add(new TestCaseSupplier("mv_count(<date_ranges>)", List.of(DataType.DATE_RANGE), () -> {
            List<LongRangeBlockBuilder.LongRange> mvData = randomList(1, 10, TestCaseSupplier::randomDateRange);
            return new TestCaseSupplier.TestCase(
                List.of(new TestCaseSupplier.TypedData(mvData, DataType.DATE_RANGE, "field").withAppliesTo(dateRangeAppliesTo)),
                "MvCount[field=Attribute[channel=0]]",
                DataType.INTEGER,
                equalTo(mvData.size())
            );
        }));
    }
}

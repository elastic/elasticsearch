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
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesTo;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesToLifecycle;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier.appliesTo;
import static org.hamcrest.Matchers.equalTo;

public class MvFirstTests extends AbstractMultivalueFunctionTestCase {
    public MvFirstTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> cases = new ArrayList<>();
        booleans(cases, "mv_first", "MvFirst", DataType.BOOLEAN, (size, values) -> equalTo(values.findFirst().get()));
        bytesRefs(cases, "mv_first", "MvFirst", Function.identity(), (size, values) -> equalTo(values.findFirst().get()));
        flattened(cases, "mv_first", "MvFirst", Function.identity(), (size, values) -> equalTo(values.findFirst().get()));
        doubles(cases, "mv_first", "MvFirst", DataType.DOUBLE, (size, values) -> equalTo(values.findFirst().getAsDouble()));
        ints(cases, "mv_first", "MvFirst", DataType.INTEGER, (size, values) -> equalTo(values.findFirst().getAsInt()));
        longs(cases, "mv_first", "MvFirst", DataType.LONG, (size, values) -> equalTo(values.findFirst().getAsLong()));
        unsignedLongs(cases, "mv_first", "MvFirst", DataType.UNSIGNED_LONG, (size, values) -> equalTo(values.findFirst().get()));
        dateTimes(cases, "mv_first", "MvFirst", DataType.DATETIME, (size, values) -> equalTo(values.findFirst().getAsLong()));
        dateNanos(cases, "mv_first", "MvFirst", DataType.DATE_NANOS, (size, values) -> equalTo(values.findFirst().getAsLong()));
        geoPoints(cases, "mv_first", "MvFirst", DataType.GEO_POINT, (size, values) -> equalTo(values.findFirst().get()));
        cartesianPoints(cases, "mv_first", "MvFirst", DataType.CARTESIAN_POINT, (size, values) -> equalTo(values.findFirst().get()));
        geoShape(cases, "mv_first", "MvFirst", DataType.GEO_SHAPE, (size, values) -> equalTo(values.findFirst().get()));
        cartesianShape(cases, "mv_first", "MvFirst", DataType.CARTESIAN_SHAPE, (size, values) -> equalTo(values.findFirst().get()));
        geohashGrid(cases, "mv_first", "MvFirst", DataType.GEOHASH, (size, values) -> equalTo(values.findFirst().get()));
        geotileGrid(cases, "mv_first", "MvFirst", DataType.GEOTILE, (size, values) -> equalTo(values.findFirst().get()));
        geohexGrid(cases, "mv_first", "MvFirst", DataType.GEOHEX, (size, values) -> equalTo(values.findFirst().get()));
        if (EsqlCapabilities.Cap.MV_FIRST_LAST_DATE_RANGE.isEnabled()) {
            dateRanges(cases);
        }
        return parameterSuppliersFromTypedDataWithDefaultChecks(false, cases);
    }

    private static void dateRanges(List<TestCaseSupplier> cases) {
        FunctionAppliesTo dateRangeAppliesTo = appliesTo(FunctionAppliesToLifecycle.PREVIEW, "9.5.0", "", false);
        cases.add(new TestCaseSupplier("mv_first(date_range)", List.of(DataType.DATE_RANGE), () -> {
            LongRangeBlockBuilder.LongRange value = TestCaseSupplier.randomDateRange();
            return new TestCaseSupplier.TestCase(
                List.of(new TestCaseSupplier.TypedData(List.of(value), DataType.DATE_RANGE, "field").withAppliesTo(dateRangeAppliesTo)),
                "MvFirst[field=Attribute[channel=0]]",
                DataType.DATE_RANGE,
                equalTo(value)
            );
        }));
        cases.add(new TestCaseSupplier("mv_first(date_ranges)", List.of(DataType.DATE_RANGE), () -> {
            List<LongRangeBlockBuilder.LongRange> values = randomList(1, 10, () -> TestCaseSupplier.randomDateRange());
            return new TestCaseSupplier.TestCase(
                List.of(new TestCaseSupplier.TypedData(values, DataType.DATE_RANGE, "field").withAppliesTo(dateRangeAppliesTo)),
                "MvFirst[field=Attribute[channel=0]]",
                DataType.DATE_RANGE,
                equalTo(values.get(0))
            );
        }));
    }

    @Override
    protected Expression build(Source source, Expression field) {
        return new MvFirst(source, field);
    }
}

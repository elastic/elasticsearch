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
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class MvDedupeTests extends AbstractMultivalueFunctionTestCase {
    public MvDedupeTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> cases = new ArrayList<>();
        booleans(cases, "mv_dedupe", "MvDedupe", (size, values) -> getMatcher(values));
        bytesRefs(cases, "mv_dedupe", "MvDedupe", (size, values) -> getMatcher(values));
        dateTimes(cases, "mv_dedupe", "MvDedupe", (size, values) -> getMatcher(values.mapToObj(Long::valueOf)));
        doubles(cases, "mv_dedupe", "MvDedupe", (size, values) -> getMatcher(values.mapToObj(Double::valueOf)));
        ints(cases, "mv_dedupe", "MvDedupe", (size, values) -> getMatcher(values.mapToObj(Integer::valueOf)));
        longs(cases, "mv_dedupe", "MvDedupe", (size, values) -> getMatcher(values.mapToObj(Long::valueOf)));
        cartesianPoints(cases, "mv_dedupe", "MvDedupe", (size, values) -> getMatcher(values));
        cartesianShape(cases, "mv_dedupe", "MvDedupe", DataType.CARTESIAN_SHAPE, (size, values) -> getMatcher(values));
        geoPoints(cases, "mv_dedupe", "MvDedupe", (size, values) -> getMatcher(values));
        geoShape(cases, "mv_dedupe", "MvDedupe", DataType.GEO_SHAPE, (size, values) -> getMatcher(values));

        // TODO switch extraction to BigInteger so this just works.
        // unsignedLongs(cases, "mv_dedupe", "MvDedupe", (size, values) -> getMatcher(values));
        return parameterSuppliersFromTypedData(anyNullIsNull(false, cases));
    }

    @Override
    protected Expression build(Source source, Expression field) {
        return new MvDedupe(source, field);
    }

    @SuppressWarnings("unchecked")
    private static Matcher<Object> getMatcher(Stream<?> v) {
        Set<Object> values = v.collect(Collectors.toSet());
        return switch (values.size()) {
            case 0 -> nullValue();
            case 1 -> equalTo(values.iterator().next());
            default -> (Matcher<Object>) (Matcher<?>) containsInAnyOrder(values.stream().map(Matchers::equalTo).toArray(Matcher[]::new));
        };
    }
}

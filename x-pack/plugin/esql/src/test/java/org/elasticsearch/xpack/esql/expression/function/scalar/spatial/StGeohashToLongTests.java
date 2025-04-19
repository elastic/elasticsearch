/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.spatial;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.geometry.utils.Geohash;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.FunctionName;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes.UNSPECIFIED;

@FunctionName("st_geohash_to_long")
public class StGeohashToLongTests extends SpatialGridTypeConversionTestCases {
    public StGeohashToLongTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        final List<TestCaseSupplier> suppliers = new ArrayList<>();
        forUnaryGeoPoint(
            DataType.LONG,
            suppliers,
            "Attribute[channel=0]",
            DataType.LONG,
            g -> StGeohash.unboundedGrid.calculateGridId(UNSPECIFIED.wkbAsPoint(g), 2),
            StGeohashToLongTests::valueOf
        );
        forUnaryGeoPoint(
            DataType.KEYWORD,
            suppliers,
            "StGeohashToLongFromStringEvaluator[gridId=Attribute[channel=0]]",
            DataType.LONG,
            g -> new BytesRef(Geohash.stringEncode(StGeohash.unboundedGrid.calculateGridId(UNSPECIFIED.wkbAsPoint(g), 2))),
            StGeohashToLongTests::valueOf
        );
        return parameterSuppliersFromTypedDataWithDefaultChecksNoErrors(true, suppliers);
    }

    private static long valueOf(Object gridid) {
        return (gridid instanceof Long hash) ? hash : Geohash.longEncode(((BytesRef) gridid).utf8ToString());
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new StGeohashToLong(source, args.get(0));
    }
}

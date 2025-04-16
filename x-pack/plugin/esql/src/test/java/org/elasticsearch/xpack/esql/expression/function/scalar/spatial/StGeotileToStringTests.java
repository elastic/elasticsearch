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
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoTileUtils;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.FunctionName;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes.UNSPECIFIED;

@FunctionName("st_geotile_to_string")
public class StGeotileToStringTests extends SpatialGridTypeConversionTestCases {
    public StGeotileToStringTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        final List<TestCaseSupplier> suppliers = new ArrayList<>();
        forUnaryGeoPoint(
            DataType.LONG,
            suppliers,
            "StGeotileToStringFromLongEvaluator[gridId=Attribute[channel=0]]",
            KEYWORD,
            g -> StGeotile.unboundedGrid.calculateGridId(UNSPECIFIED.wkbAsPoint(g), 2),
            StGeotileToStringTests::valueOf
        );
        forUnaryGeoPoint(
            DataType.KEYWORD,
            suppliers,
            "Attribute[channel=0]",
            KEYWORD,
            g -> new BytesRef(GeoTileUtils.stringEncode(StGeotile.unboundedGrid.calculateGridId(UNSPECIFIED.wkbAsPoint(g), 2))),
            StGeotileToStringTests::valueOf
        );
        return parameterSuppliersFromTypedDataWithDefaultChecksNoErrors(true, suppliers);
    }

    private static BytesRef valueOf(Object gridid) {
        return (gridid instanceof Long hash) ? new BytesRef(GeoTileUtils.stringEncode(hash)) : (BytesRef) gridid;
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new StGeotileToString(source, args.get(0));
    }
}

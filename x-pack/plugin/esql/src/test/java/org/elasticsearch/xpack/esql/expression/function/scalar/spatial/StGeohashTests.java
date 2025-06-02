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
import org.elasticsearch.common.geo.GeoBoundingBox;
import org.elasticsearch.geometry.utils.Geohash;
import org.elasticsearch.license.License;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes.UNSPECIFIED;
import static org.hamcrest.Matchers.containsString;

public class StGeohashTests extends SpatialGridFunctionTestCase {
    public StGeohashTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    /**
     * Since geo grid functions are primarily used for spatial aggregations,
     * we use the same license requirement as the spatial aggregations.
     */
    public static License.OperationMode licenseRequirement(List<DataType> fieldTypes) {
        return SpatialGridFunctionTestCase.licenseRequirement(fieldTypes);
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        final List<TestCaseSupplier> suppliers = new ArrayList<>();
        addTestCaseSuppliers(suppliers, new DataType[] { DataType.GEO_POINT }, StGeohashTests::valueOf, StGeohashTests::boundedValueOf);
        return parameterSuppliersFromTypedDataWithDefaultChecksNoErrors(true, suppliers);
    }

    private static long valueOf(BytesRef wkb, int precision) {
        return StGeohash.unboundedGrid.calculateGridId(UNSPECIFIED.wkbAsPoint(wkb), precision);
    }

    private static long boundedValueOf(BytesRef wkb, int precision, GeoBoundingBox bbox) {
        StGeohash.GeoHashBoundedGrid bounds = new StGeohash.GeoHashBoundedGrid(precision, bbox);
        return bounds.calculateGridId(UNSPECIFIED.wkbAsPoint(wkb));
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        Expression bounds = args.size() > 2 ? args.get(2) : null;
        return new StGeohash(source, args.get(0), args.get(1), bounds);
    }

    public void testInvalidPrecision() {
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> process(-1, StGeohashTests::valueOf));
        assertThat(ex.getMessage(), containsString("Invalid geohash_grid precision of -1. Must be between 1 and 12."));
        ex = expectThrows(IllegalArgumentException.class, () -> process(Geohash.PRECISION + 1, StGeohashTests::valueOf));
        assertThat(ex.getMessage(), containsString("Invalid geohash_grid precision of 13. Must be between 1 and 12."));
    }
}

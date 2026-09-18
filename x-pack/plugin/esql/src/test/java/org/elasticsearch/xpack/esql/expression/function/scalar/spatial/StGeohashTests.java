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
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.utils.Geohash;
import org.elasticsearch.license.License;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.esql.core.type.DataType.GEOHASH;
import static org.elasticsearch.xpack.esql.core.type.DataType.GEO_POINT;
import static org.elasticsearch.xpack.esql.core.type.DataType.GEO_SHAPE;
import static org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes.GEO;
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
        addTestCaseSuppliers(
            suppliers,
            new DataType[] { GEO_POINT, GEO_SHAPE },
            GEOHASH,
            StGeohashTests::valueOf,
            StGeohashTests::boundedValueOf
        );
        return parameterSuppliersFromTypedDataWithDefaultChecks(true, suppliers);
    }

    private static Object valueOf(BytesRef wkb, int precision, Consumer<String> warnings) {
        Geometry geometry = GEO.wkbToGeometry(wkb);
        if (geometry instanceof Point point) {
            return StGeohash.unboundedGrid.calculateGridId(point, precision);
        }
        try {
            return SpatialGridFunction.foldMultiValue(StGeohash.computeGeohashCells(wkb, precision, null, warnings));
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to compute geohash for geo_shape", e);
        }
    }

    private static Object boundedValueOf(BytesRef wkb, int precision, GeoBoundingBox bbox, Consumer<String> warnings) {
        Geometry geometry = GEO.wkbToGeometry(wkb);
        if (geometry instanceof Point point) {
            StGeohash.GeoHashBoundedGrid bounds = new StGeohash.GeoHashBoundedGrid.Factory(precision, bbox).get(null);
            long gridId = bounds.calculateGridId(point);
            return gridId < 0 ? null : gridId;
        }
        try {
            return SpatialGridFunction.foldMultiValue(StGeohash.computeGeohashCells(wkb, precision, bbox, warnings));
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to compute geohash for geo_shape", e);
        }
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

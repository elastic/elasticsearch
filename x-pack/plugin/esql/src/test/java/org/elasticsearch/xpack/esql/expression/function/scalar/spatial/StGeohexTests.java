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
import org.elasticsearch.h3.H3;
import org.elasticsearch.license.License;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.esql.core.type.DataType.GEOHEX;
import static org.elasticsearch.xpack.esql.core.type.DataType.GEO_POINT;
import static org.elasticsearch.xpack.esql.core.type.DataType.GEO_SHAPE;
import static org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes.GEO;
import static org.hamcrest.Matchers.containsString;

public class StGeohexTests extends SpatialGridFunctionTestCase {
    public StGeohexTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    /**
     * Other geo grid functions use the same type-specific license requirement as the spatial aggregations, but geohex is licensed
     * more strictly, at platinum for all field types.
     */
    public static License.OperationMode licenseRequirement(List<DataType> fieldTypes) {
        return License.OperationMode.PLATINUM;
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        final List<TestCaseSupplier> suppliers = new ArrayList<>();
        addTestCaseSuppliers(
            suppliers,
            new DataType[] { GEO_POINT, GEO_SHAPE },
            GEOHEX,
            StGeohexTests::valueOf,
            StGeohexTests::boundedValueOf
        );
        return parameterSuppliersFromTypedDataWithDefaultChecks(true, suppliers);
    }

    private static Object valueOf(BytesRef wkb, int precision) {
        Geometry geometry = GEO.wkbToGeometry(wkb);
        if (geometry instanceof Point point) {
            return StGeohex.unboundedGrid.calculateGridId(point, precision);
        }
        try {
            long[] cells = StGeohex.computeGeohexCells(wkb, precision, null);
            return SpatialGridFunction.foldMultiValue(cells);
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to compute geohex for geo_shape", e);
        }
    }

    private static Object boundedValueOf(BytesRef wkb, int precision, GeoBoundingBox bbox) {
        Geometry geometry = GEO.wkbToGeometry(wkb);
        if (geometry instanceof Point point) {
            StGeohex.GeoHexBoundedGrid bounds = new StGeohex.GeoHexBoundedGrid.Factory(precision, bbox).get(null);
            long gridId = bounds.calculateGridId(point);
            return gridId < 0 ? null : gridId;
        }
        try {
            long[] cells = StGeohex.computeGeohexCells(wkb, precision, bbox);
            return SpatialGridFunction.foldMultiValue(cells);
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to compute geohex for geo_shape", e);
        }
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        Expression bounds = args.size() > 2 ? args.get(2) : null;
        return new StGeohex(source, args.get(0), args.get(1), bounds);
    }

    public void testInvalidPrecision() {
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> process(-1, StGeohexTests::valueOf));
        assertThat(ex.getMessage(), containsString("Invalid geohex_grid precision of -1. Must be between 0 and 15."));
        ex = expectThrows(IllegalArgumentException.class, () -> process(H3.MAX_H3_RES + 1, StGeohexTests::valueOf));
        assertThat(ex.getMessage(), containsString("Invalid geohex_grid precision of 16. Must be between 0 and 15."));
    }
}

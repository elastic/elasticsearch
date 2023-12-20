/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.search;

import org.elasticsearch.common.geo.GeometryNormalizer;
import org.elasticsearch.common.geo.Orientation;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.GeometryCollection;
import org.elasticsearch.geometry.Line;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.Polygon;
import org.elasticsearch.search.geo.BaseShapeQueryTestCase;
import org.elasticsearch.search.geo.SpatialQueryBuilders;
import org.elasticsearch.xpack.spatial.index.query.ShapeQueryBuilder;
import org.elasticsearch.xpack.spatial.util.ShapeTestUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public abstract class CartesianShapeQueryTestCase extends BaseShapeQueryTestCase<ShapeQueryBuilder> {

    private final SpatialQueryBuilders<ShapeQueryBuilder> shapeQueryBuilder = TestSpatialQueryBuilders.CARTESIAN;

    @Override
    protected SpatialQueryBuilders<ShapeQueryBuilder> queryBuilder() {
        return shapeQueryBuilder;
    }

    @Override
    protected String fieldTypeName() {
        return "shape";
    }

    protected Line makeRandomLine() {
        return randomValueOtherThanMany(l -> GeometryNormalizer.needsNormalize(Orientation.CCW, l), () -> ShapeTestUtils.randomLine(false));
    }

    protected Polygon makeRandomPolygon() {
        return randomValueOtherThanMany(
            p -> GeometryNormalizer.needsNormalize(Orientation.CCW, p),
            () -> ShapeTestUtils.randomPolygon(false)
        );
    }

    protected GeometryCollection<Geometry> makeRandomGeometryCollection() {
        return ShapeTestUtils.randomGeometryCollection(false);
    }

    protected GeometryCollection<Geometry> makeRandomGeometryCollectionWithoutCircle(Geometry... extra) {
        GeometryCollection<Geometry> randomCollection = ShapeTestUtils.randomGeometryCollectionWithoutCircle(false);
        if (extra.length == 0) return randomCollection;

        List<Geometry> geometries = new ArrayList<>();
        for (Geometry geometry : randomCollection) {
            geometries.add(geometry);
        }
        Collections.addAll(geometries, extra);
        return new GeometryCollection<>(geometries);
    }

    protected Point nextPoint() {
        return ShapeTestUtils.randomPoint(false);
    }

    protected Polygon nextPolygon() {
        return ShapeTestUtils.randomPolygon(false);
    }

    protected Polygon nextPolygon2() {
        return ShapeTestUtils.randomPolygon(false);
    }
}

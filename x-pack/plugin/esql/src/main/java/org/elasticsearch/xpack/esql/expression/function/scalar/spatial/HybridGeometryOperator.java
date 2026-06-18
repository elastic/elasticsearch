/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.spatial;

import org.apache.lucene.document.ShapeField;
import org.apache.lucene.geo.Component2D;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.geo.Orientation;
import org.elasticsearch.geometry.GeometryCollection;
import org.elasticsearch.index.mapper.GeoShapeIndexer;
import org.elasticsearch.index.mapper.ShapeIndexer;
import org.elasticsearch.lucene.spatial.CartesianShapeIndexer;
import org.elasticsearch.lucene.spatial.Component2DVisitor;
import org.elasticsearch.lucene.spatial.CoordinateEncoder;
import org.elasticsearch.lucene.spatial.GeometryDocValueReader;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes.UNSPECIFIED;

/**
 * A {@link GeometryOperator} that uses a Lucene triangle-tree pre-check to detect disjoint
 * geometries and return a fast shortcut result (WKB), falling through to a wrapped JTS
 * {@link GeometryOperator} when the geometries may intersect.
 *
 * <p>Both inputs and the return value are WKB-encoded ({@link BytesRef}). JTS conversions are
 * deferred to the fallback operator and never performed when the disjoint shortcut fires.
 *
 * <p>For each pair of WKB geometries:
 * <ol>
 *   <li>Decode both WKB inputs to Elasticsearch {@link org.elasticsearch.geometry.Geometry}.</li>
 *   <li>Build a {@link GeometryDocValueReader} triangle-tree for geometry A and a
 *       {@link Component2D} for geometry B.</li>
 *   <li>Evaluate the {@link ShapeField.QueryRelation#INTERSECTS} predicate.</li>
 *   <li>If disjoint, return the pre-computed shortcut WKB result without calling JTS.</li>
 *   <li>If intersecting (or if the Lucene check throws), delegate to the fallback operator.</li>
 * </ol>
 *
 * <p>Use the static factory methods to create instances:
 * {@link #intersection(BinarySpatialFunction.SpatialCrsType)},
 * {@link #union(BinarySpatialFunction.SpatialCrsType)},
 * {@link #difference(BinarySpatialFunction.SpatialCrsType)},
 * {@link #symDifference(BinarySpatialFunction.SpatialCrsType)}.
 */
public final class HybridGeometryOperator implements GeometryOperator {

    private final CoordinateEncoder encoder;
    private final ShapeIndexer shapeIndexer;
    private final BinarySpatialFunction.SpatialCrsType crsType;
    private final GeometryOperator shortcutWhenDisjoint;
    private final GeometryOperator fallback;

    private HybridGeometryOperator(
        CoordinateEncoder encoder,
        ShapeIndexer shapeIndexer,
        BinarySpatialFunction.SpatialCrsType crsType,
        GeometryOperator shortcutWhenDisjoint,
        GeometryOperator fallback
    ) {
        this.encoder = encoder;
        this.shapeIndexer = shapeIndexer;
        this.crsType = crsType;
        this.shortcutWhenDisjoint = shortcutWhenDisjoint;
        this.fallback = fallback;
    }

    @Override
    public BytesRef apply(BytesRef wkbA, BytesRef wkbB) throws IOException {
        try {
            org.elasticsearch.geometry.Geometry esGeomA = UNSPECIFIED.wkbToGeometry(wkbA);
            org.elasticsearch.geometry.Geometry esGeomB = UNSPECIFIED.wkbToGeometry(wkbB);

            GeometryDocValueReader reader = SpatialRelatesUtils.asGeometryDocValueReader(encoder, shapeIndexer, esGeomA);
            Component2D component2d = SpatialRelatesUtils.asLuceneComponent2D(crsType, esGeomB);

            Component2DVisitor visitor = Component2DVisitor.getVisitor(component2d, ShapeField.QueryRelation.INTERSECTS, encoder);
            reader.visit(visitor);
            if (visitor.matches() == false) {
                return shortcutWhenDisjoint.apply(wkbA, wkbB);
            }
        } catch (IOException | IllegalArgumentException ignored) {
            // Fall through to JTS
        }
        return fallback.apply(wkbA, wkbB);
    }

    /**
     * Creates a {@link HybridGeometryOperator} for ST_INTERSECTION.
     * When disjoint, returns an empty geometry collection as WKB.
     */
    public static HybridGeometryOperator intersection(BinarySpatialFunction.SpatialCrsType crsType) {
        BytesRef emptyCollection = UNSPECIFIED.asWkb(new GeometryCollection<>());
        GeometryOperator shortcut = (wkbA, wkbB) -> emptyCollection;
        return new HybridGeometryOperator(encoderFor(crsType), indexerFor(crsType), crsType, shortcut, JtsGeometryOperator.INTERSECTION);
    }

    /**
     * Creates a {@link HybridGeometryOperator} for ST_UNION.
     * When disjoint, returns a geometry collection containing both inputs unchanged as WKB.
     */
    public static HybridGeometryOperator union(BinarySpatialFunction.SpatialCrsType crsType) {
        GeometryOperator shortcut = (wkbA, wkbB) -> UNSPECIFIED.asWkb(
            new GeometryCollection<>(List.of(UNSPECIFIED.wkbToGeometry(wkbA), UNSPECIFIED.wkbToGeometry(wkbB)))
        );
        return new HybridGeometryOperator(encoderFor(crsType), indexerFor(crsType), crsType, shortcut, JtsGeometryOperator.UNION);
    }

    /**
     * Creates a {@link HybridGeometryOperator} for ST_DIFFERENCE.
     * When disjoint, returns {@code wkbA} unchanged (A ∖ B = A when disjoint).
     */
    public static HybridGeometryOperator difference(BinarySpatialFunction.SpatialCrsType crsType) {
        GeometryOperator shortcut = (wkbA, wkbB) -> wkbA;
        return new HybridGeometryOperator(encoderFor(crsType), indexerFor(crsType), crsType, shortcut, JtsGeometryOperator.DIFFERENCE);
    }

    /**
     * Creates a {@link HybridGeometryOperator} for ST_SYMDIFFERENCE.
     * When disjoint, returns a geometry collection containing both inputs unchanged as WKB
     * (symmetric difference of disjoint geometries is their union).
     */
    public static HybridGeometryOperator symDifference(BinarySpatialFunction.SpatialCrsType crsType) {
        GeometryOperator shortcut = (wkbA, wkbB) -> UNSPECIFIED.asWkb(
            new GeometryCollection<>(List.of(UNSPECIFIED.wkbToGeometry(wkbA), UNSPECIFIED.wkbToGeometry(wkbB)))
        );
        return new HybridGeometryOperator(encoderFor(crsType), indexerFor(crsType), crsType, shortcut, JtsGeometryOperator.SYM_DIFFERENCE);
    }

    private static CoordinateEncoder encoderFor(BinarySpatialFunction.SpatialCrsType crsType) {
        return switch (crsType) {
            case GEO -> CoordinateEncoder.GEO;
            case CARTESIAN -> CoordinateEncoder.CARTESIAN;
            case UNSPECIFIED -> throw new IllegalArgumentException("UNSPECIFIED CRS type is not supported");
        };
    }

    private static ShapeIndexer indexerFor(BinarySpatialFunction.SpatialCrsType crsType) {
        return switch (crsType) {
            case GEO -> new GeoShapeIndexer(Orientation.CCW, "HybridGeometryOperator");
            case CARTESIAN -> new CartesianShapeIndexer("HybridGeometryOperator");
            case UNSPECIFIED -> throw new IllegalArgumentException("UNSPECIFIED CRS type is not supported");
        };
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.runtime;

import org.apache.lucene.geo.Component2D;
import org.apache.lucene.geo.GeoEncodingUtils;
import org.apache.lucene.geo.LatLonGeometry;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.script.GeoPointFieldScript;
import org.elasticsearch.script.Script;

import java.util.Arrays;
import java.util.Objects;

public class GeoPointScriptFieldGeoShapeQuery extends AbstractGeoPointScriptFieldQuery {

    private final SpatialPredicate predicate;
    private final LatLonGeometry[] geometries;
    private final ShapeRelation relation;

    public GeoPointScriptFieldGeoShapeQuery(
        Script script,
        GeoPointFieldScript.LeafFactory leafFactory,
        String fieldName,
        ShapeRelation relation,
        LatLonGeometry... geometries
    ) {
        super(script, leafFactory, fieldName);
        this.geometries = geometries;
        this.relation = relation;
        this.predicate = getPredicate(relation, geometries);
    }

    @Override
    protected boolean matches(long[] values, int count) {
        return predicate.matches(values, count);
    }

    @Override
    public final String toString(String field) {
        if (fieldName().contentEquals(field)) {
            return getClass().getSimpleName();
        }
        return fieldName() + ":" + getClass().getSimpleName();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (super.equals(o) == false) return false;
        GeoPointScriptFieldGeoShapeQuery that = (GeoPointScriptFieldGeoShapeQuery) o;
        return relation == that.relation && Arrays.equals(geometries, that.geometries);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), relation, Arrays.hashCode(geometries));
    }

    @FunctionalInterface
    private interface SpatialPredicate {
        boolean matches(long[] values, int count);
    }

    private static SpatialPredicate getPredicate(ShapeRelation relation, LatLonGeometry... geometries) {
        switch (relation) {
            case INTERSECTS: {
                final GeoEncodingUtils.Component2DPredicate predicate = GeoEncodingUtils.createComponentPredicate(
                    LatLonGeometry.create(geometries)
                );
                return (values, count) -> {
                    for (int i = 0; i < count; i++) {
                        final long value = values[i];
                        final int lat = (int) (value >>> 32);
                        final int lon = (int) (value & 0xFFFFFFFF);
                        if (predicate.test(lat, lon)) {
                            return true;
                        }
                    }
                    return false;
                };
            }
            case DISJOINT: {
                final GeoEncodingUtils.Component2DPredicate predicate = GeoEncodingUtils.createComponentPredicate(
                    LatLonGeometry.create(geometries)
                );
                return (values, count) -> {
                    for (int i = 0; i < count; i++) {
                        final long value = values[i];
                        final int lat = (int) (value >>> 32);
                        final int lon = (int) (value & 0xFFFFFFFF);
                        if (predicate.test(lat, lon)) {
                            return false;
                        }
                    }
                    // return true iff there is at least one point
                    return count > 0;
                };
            }
            case WITHIN: {
                final GeoEncodingUtils.Component2DPredicate predicate = GeoEncodingUtils.createComponentPredicate(
                    LatLonGeometry.create(geometries)
                );
                return (values, count) -> {
                    for (int i = 0; i < count; i++) {
                        final long value = values[i];
                        final int lat = (int) (value >>> 32);
                        final int lon = (int) (value & 0xFFFFFFFF);
                        if (predicate.test(lat, lon) == false) {
                            return false;
                        }
                    }
                    // return true iff there is at least one point
                    return count > 0;
                };
            }
            case CONTAINS: {
                final Component2D[] component2DS = new Component2D[geometries.length];
                for (int i = 0; i < geometries.length; i++) {
                    component2DS[i] = LatLonGeometry.create(geometries[i]);
                }
                return (values, count) -> {
                    Component2D.WithinRelation answer = Component2D.WithinRelation.DISJOINT;
                    for (int i = 0; i < count; i++) {
                        final long value = values[i];
                        final double lat = GeoEncodingUtils.decodeLatitude((int) (value >>> 32));
                        final double lon = GeoEncodingUtils.decodeLongitude((int) (value & 0xFFFFFFFF));
                        for (Component2D component2D : component2DS) {
                            Component2D.WithinRelation withinRelation = component2D.withinPoint(lon, lat);
                            if (withinRelation == Component2D.WithinRelation.NOTWITHIN) {
                                return false;
                            } else if (withinRelation != Component2D.WithinRelation.DISJOINT) {
                                answer = withinRelation;
                            }
                        }
                    }
                    return answer == Component2D.WithinRelation.CANDIDATE;
                };
            }
            default:
                throw new IllegalArgumentException("Unknown spatial relationship [" + relation.getRelationName() + "]");
        }
    }
}

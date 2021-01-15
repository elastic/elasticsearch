/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.runtimefields.query;

import org.apache.lucene.geo.GeoEncodingUtils;
import org.apache.lucene.geo.LatLonGeometry;
import org.elasticsearch.script.Script;
import org.elasticsearch.xpack.runtimefields.mapper.GeoPointFieldScript;

import java.util.Arrays;
import java.util.Objects;

public class GeoPointScriptFieldGeoShapeQuery extends AbstractGeoPointScriptFieldQuery {

    private final GeoEncodingUtils.Component2DPredicate predicate;
    private final LatLonGeometry[] geometries;

    public GeoPointScriptFieldGeoShapeQuery(
        Script script,
        GeoPointFieldScript.LeafFactory leafFactory,
        String fieldName,
        LatLonGeometry... geometries
    ) {
        super(script, leafFactory, fieldName);
        this.geometries = geometries;
        predicate = GeoEncodingUtils.createComponentPredicate(LatLonGeometry.create(geometries));
    }

    @Override
    protected boolean matches(long[] values, int count) {
        for (int i = 0; i < count; i++) {
            final long value = values[i];
            final int lat = (int) (value >>> 32);
            final int lon = (int) (value & 0xFFFFFFFF);
            if (predicate.test(lat, lon)) {
                return true;
            }
        }
        return false;
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
        if (!super.equals(o)) return false;
        GeoPointScriptFieldGeoShapeQuery that = (GeoPointScriptFieldGeoShapeQuery) o;
        return Arrays.equals(geometries, that.geometries);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), Arrays.hashCode(geometries));
    }
}

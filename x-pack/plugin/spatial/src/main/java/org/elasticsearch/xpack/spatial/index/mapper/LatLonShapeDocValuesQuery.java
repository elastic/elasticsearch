/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.index.mapper;

import org.apache.lucene.document.ShapeField;
import org.apache.lucene.geo.Component2D;
import org.apache.lucene.geo.LatLonGeometry;
import org.apache.lucene.geo.Rectangle;
import org.elasticsearch.xpack.spatial.index.fielddata.CoordinateEncoder;

import java.util.List;

/** Lucene geometry query for {@link BinaryShapeDocValuesField}. */
class LatLonShapeDocValuesQuery extends ShapeDocValuesQuery<LatLonGeometry> {

    LatLonShapeDocValuesQuery(String field, ShapeField.QueryRelation relation, LatLonGeometry... geometries) {
        super(field, CoordinateEncoder.GEO, relation, geometries);
    }

    @Override
    protected Component2D create(LatLonGeometry[] geometries) {
        return LatLonGeometry.create(geometries);
    }

    @Override
    protected Component2D create(LatLonGeometry geometry) {
        return LatLonGeometry.create(geometry);
    }

    @Override
    protected void add(List<Component2D> components2D, LatLonGeometry geometry) {
        if (geometry instanceof Rectangle r && r.minLon > r.maxLon) {
            super.add(components2D, new Rectangle(r.minLat, r.maxLat, r.minLon, 180));
            super.add(components2D, new Rectangle(r.minLat, r.maxLat, -180, r.maxLon));
        } else {
            super.add(components2D, geometry);
        }
    }
}

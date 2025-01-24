/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.lucene.spatial;

import org.apache.lucene.document.ShapeField;
import org.apache.lucene.geo.Component2D;
import org.apache.lucene.geo.LatLonGeometry;
import org.apache.lucene.geo.Rectangle;

import java.util.List;

/** Lucene geometry query for {@link BinaryShapeDocValuesField}. */
public class LatLonShapeDocValuesQuery extends ShapeDocValuesQuery<LatLonGeometry> {

    public LatLonShapeDocValuesQuery(String field, ShapeField.QueryRelation relation, LatLonGeometry... geometries) {
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

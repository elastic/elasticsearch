/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.vectortile.collector;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.index.fielddata.LeafGeoPointFieldData;
import org.elasticsearch.index.fielddata.MultiGeoPointValues;
import org.elasticsearch.index.fielddata.plain.AbstractLatLonPointIndexFieldData;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.GeometryFactory;

/**
 * builds Vector Tile object from source documents. Geometries are expected to be
 * in WKT format.
 */
public class PointDocValuesMapBoxCollector extends AbstractMapBoxVectorTileCollector {

    final AbstractLatLonPointIndexFieldData.LatLonPointIndexFieldData points;
    GeometryFactory geomFactory = new GeometryFactory();

    public PointDocValuesMapBoxCollector(AbstractLatLonPointIndexFieldData.LatLonPointIndexFieldData points,
                                         Envelope tileEnvelope, String field) {
        super(tileEnvelope, field);
        this.points = points;
    }

    @Override
    public MapBoxVectorTileLeafCollector getVectorTileLeafCollector(LeafReaderContext context) {
        LeafGeoPointFieldData data = points.load(context);
        MultiGeoPointValues values = data.getGeoPointValues();
        return docID -> {
            values.advanceExact(docID);
            GeoPoint point = values.nextValue();
            return  geomFactory.createPoint(new Coordinate(point.lon(), point.lat()));
        };
    }
}

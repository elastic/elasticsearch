/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.index.fielddata;

import org.apache.lucene.geo.Component2D;
import org.apache.lucene.geo.LatLonGeometry;
import org.apache.lucene.geo.Point;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.Orientation;
import org.elasticsearch.index.mapper.GeoShapeIndexer;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.xpack.spatial.search.aggregations.support.GeoShapeValuesSourceType;

import java.io.IOException;

/**
 * A stateful lightweight per document geo values.
 * To iterate over values in a document use the following pattern:
 * <pre>
 *   MultiGeoValues values = ..;
 *   // for each docID
 *   if (values.advanceExact(docId)) {
 *     GeoValue value = values.value()
 *     final int numValues = values.count();
 *     // process value
 *   }
 * </pre>
 *
 * There is just one value for one document.
 */
public abstract class GeoShapeValues extends ShapeValues {

    public static GeoShapeValues EMPTY = new GeoShapeValues() {
        private final GeoShapeValuesSourceType DEFAULT_VALUES_SOURCE_TYPE = GeoShapeValuesSourceType.instance();

        @Override
        public boolean advanceExact(int doc) {
            return false;
        }

        @Override
        public ValuesSourceType valuesSourceType() {
            return DEFAULT_VALUES_SOURCE_TYPE;
        }

        @Override
        public GeoShapeValue value() {
            throw new UnsupportedOperationException();
        }
    };

    /**
     * Creates a new {@link GeoShapeValues} instance
     */
    protected GeoShapeValues() {
        super(CoordinateEncoder.GEO, GeoShapeValues.GeoShapeValue::new, new GeoShapeIndexer(Orientation.CCW, "missing"));
    }

    /** thin wrapper around a {@link GeometryDocValueReader} which encodes / decodes values using
     * the Geo decoder */
    public static class GeoShapeValue extends ShapeValues.ShapeValue {
        public GeoShapeValue() {
            super(CoordinateEncoder.GEO, (x, y) -> new GeoPoint(y, x));
        }

        @Override
        protected Component2D centroidAsComponent2D() throws IOException {
            return LatLonGeometry.create(new Point(getY(), getX()));
        }
    }
}

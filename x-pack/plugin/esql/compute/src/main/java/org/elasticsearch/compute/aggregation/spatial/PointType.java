/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation.spatial;

import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.geometry.utils.SpatialEnvelopeVisitor;
import org.elasticsearch.geometry.utils.SpatialEnvelopeVisitor.WrapLongitude;
import org.elasticsearch.lucene.spatial.CoordinateEncoder;

import java.util.Optional;

public enum PointType {
    GEO {
        @Override
        public Optional<Rectangle> computeEnvelope(Geometry geo) {
            return SpatialEnvelopeVisitor.visitGeo(geo, WrapLongitude.WRAP);
        }

        // Geo encodes the longitude in the lower 32 bits and the latitude in the upper 32 bits.
        @Override
        public int extractX(long encoded) {
            return SpatialAggregationUtils.extractSecond(encoded);
        }

        @Override
        public int extractY(long encoded) {
            return SpatialAggregationUtils.extractFirst(encoded);
        }

        @Override
        public CoordinateEncoder encoder() {
            return CoordinateEncoder.GEO;
        }
    },
    CARTESIAN {
        @Override
        public Optional<Rectangle> computeEnvelope(Geometry geo) {
            return SpatialEnvelopeVisitor.visitCartesian(geo);
        }

        @Override
        public int extractX(long encoded) {
            return SpatialAggregationUtils.extractFirst(encoded);
        }

        @Override
        public int extractY(long encoded) {
            return SpatialAggregationUtils.extractSecond(encoded);
        }

        @Override
        public CoordinateEncoder encoder() {
            return CoordinateEncoder.CARTESIAN;
        }
    };

    public abstract Optional<Rectangle> computeEnvelope(Geometry geo);

    public abstract int extractX(long encoded);

    public abstract int extractY(long encoded);

    public abstract CoordinateEncoder encoder();
}

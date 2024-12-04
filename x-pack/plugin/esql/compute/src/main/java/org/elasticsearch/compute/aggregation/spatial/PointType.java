/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation.spatial;

import org.apache.lucene.geo.GeoEncodingUtils;
import org.apache.lucene.geo.XYEncodingUtils;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.geometry.utils.SpatialEnvelopeVisitor;
import org.elasticsearch.geometry.utils.SpatialEnvelopeVisitor.WrapLongitude;

import java.util.Optional;

public enum PointType {
    GEO {
        @Override
        public Optional<Rectangle> computeEnvelope(Geometry geo) {
            return SpatialEnvelopeVisitor.visitGeo(geo, WrapLongitude.WRAP);
        }

        @Override
        public double decodeX(int encoded) {
            return GeoEncodingUtils.decodeLongitude(encoded);
        }

        @Override
        public double decodeY(int encoded) {
            return GeoEncodingUtils.decodeLatitude(encoded);
        }

        @Override
        public int encodeX(double decoded) {
            return GeoEncodingUtils.encodeLongitude(decoded);
        }

        @Override
        public int encodeY(double decoded) {
            return GeoEncodingUtils.encodeLatitude(decoded);
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
    },
    CARTESIAN {
        @Override
        public Optional<Rectangle> computeEnvelope(Geometry geo) {
            return SpatialEnvelopeVisitor.visitCartesian(geo);
        }

        @Override
        public double decodeX(int encoded) {
            return XYEncodingUtils.decode(encoded);
        }

        @Override
        public double decodeY(int encoded) {
            return XYEncodingUtils.decode(encoded);
        }

        @Override
        public int encodeX(double decoded) {
            return XYEncodingUtils.encode((float) decoded);
        }

        @Override
        public int encodeY(double decoded) {
            return XYEncodingUtils.encode((float) decoded);
        }

        @Override
        public int extractX(long encoded) {
            return SpatialAggregationUtils.extractFirst(encoded);
        }

        @Override
        public int extractY(long encoded) {
            return SpatialAggregationUtils.extractSecond(encoded);
        }
    };

    public abstract Optional<Rectangle> computeEnvelope(Geometry geo);

    public abstract double decodeX(int encoded);

    public abstract double decodeY(int encoded);

    public abstract int encodeX(double decoded);

    public abstract int encodeY(double decoded);

    public abstract int extractX(long encoded);

    public abstract int extractY(long encoded);
}

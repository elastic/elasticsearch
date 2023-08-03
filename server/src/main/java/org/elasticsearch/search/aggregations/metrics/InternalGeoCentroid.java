/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.metrics;

import org.apache.lucene.geo.GeoEncodingUtils;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.SpatialPoint;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.support.SamplingContext;
import org.elasticsearch.xcontent.ParseField;

import java.io.IOException;
import java.util.Map;

/**
 * Serialization and merge logic for {@link GeoCentroidAggregator}.
 */
public class InternalGeoCentroid extends InternalCentroid implements GeoCentroid {

    private static long encodeLatLon(double lat, double lon) {
        return (Integer.toUnsignedLong(GeoEncodingUtils.encodeLatitude(lat)) << 32) | Integer.toUnsignedLong(
            GeoEncodingUtils.encodeLongitude(lon)
        );
    }

    private static double decodeLatitude(long encodedLatLon) {
        return GeoEncodingUtils.decodeLatitude((int) (encodedLatLon >>> 32));
    }

    private static double decodeLongitude(long encodedLatLon) {
        return GeoEncodingUtils.decodeLongitude((int) (encodedLatLon & 0xFFFFFFFFL));
    }

    public InternalGeoCentroid(String name, SpatialPoint centroid, long count, Map<String, Object> metadata) {
        super(
            name,
            centroid,
            count,
            metadata,
            new FieldExtractor("lat", SpatialPoint::getY),
            new FieldExtractor("lon", SpatialPoint::getX)
        );
    }

    /**
     * Read from a stream.
     */
    public InternalGeoCentroid(StreamInput in) throws IOException {
        super(in, new FieldExtractor("lat", SpatialPoint::getY), new FieldExtractor("lon", SpatialPoint::getX));
    }

    public static InternalGeoCentroid empty(String name, Map<String, Object> metadata) {
        return new InternalGeoCentroid(name, null, 0L, metadata);
    }

    @Override
    protected GeoPoint centroidFromStream(StreamInput in) throws IOException {
        if (in.getTransportVersion().onOrAfter(TransportVersion.V_7_2_0)) {
            return new GeoPoint(in.readDouble(), in.readDouble());
        } else {
            final long hash = in.readLong();
            return new GeoPoint(decodeLatitude(hash), decodeLongitude(hash));
        }
    }

    @Override
    protected void centroidToStream(StreamOutput out) throws IOException {
        if (out.getTransportVersion().onOrAfter(TransportVersion.V_7_2_0)) {
            out.writeDouble(centroid.getY());
            out.writeDouble(centroid.getX());
        } else {
            out.writeLong(encodeLatLon(centroid.getY(), centroid.getX()));
        }
    }

    @Override
    public String getWriteableName() {
        return GeoCentroidAggregationBuilder.NAME;
    }

    @Override
    protected double extractDouble(String name) {
        return switch (name) {
            case "lat" -> centroid.getY();
            case "lon" -> centroid.getX();
            default -> throw new IllegalArgumentException("Found unknown path element [" + name + "] in [" + getName() + "]");
        };
    }

    @Override
    protected InternalGeoCentroid copyWith(SpatialPoint result, long count) {
        return new InternalGeoCentroid(name, result, count, getMetadata());
    }

    @Override
    protected InternalGeoCentroid copyWith(double firstSum, double secondSum, long totalCount) {
        final GeoPoint result = (Double.isNaN(firstSum)) ? null : new GeoPoint(firstSum / totalCount, secondSum / totalCount);
        return copyWith(result, totalCount);
    }

    @Override
    public InternalAggregation finalizeSampling(SamplingContext samplingContext) {
        return new InternalGeoCentroid(name, centroid, samplingContext.scaleUp(count), getMetadata());
    }

    static class Fields {
        static final ParseField CENTROID_LAT = new ParseField("lat");
        static final ParseField CENTROID_LON = new ParseField("lon");
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.SpatialPoint;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Map;

/**
 * Serialization and merge logic for {@link GeoCentroidAggregator}.
 */
public class InternalGeoCentroid extends InternalCentroid implements GeoCentroid {

    public InternalGeoCentroid(String name, SpatialPoint centroid, long count, Map<String, Object> metadata) {
        super(name, centroid, count, metadata);
    }

    /**
     * Read from a stream.
     */
    public InternalGeoCentroid(StreamInput in) throws IOException {
        super(in);
    }

    public static InternalGeoCentroid empty(String name, Map<String, Object> metadata) {
        return new InternalGeoCentroid(name, null, 0L, metadata);
    }

    @Override
    protected GeoPoint centroidFromStream(StreamInput in) throws IOException {
        return new GeoPoint(in.readDouble(), in.readDouble());
    }

    @Override
    protected void centroidToStream(StreamOutput out) throws IOException {
        out.writeDouble(centroid.getY());
        out.writeDouble(centroid.getX());
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
    protected String nameFirst() {
        return "lat";
    }

    @Override
    protected double extractFirst(SpatialPoint point) {
        return point.getY();
    }

    @Override
    protected String nameSecond() {
        return "lon";
    }

    @Override
    protected double extractSecond(SpatialPoint point) {
        return point.getX();
    }
}

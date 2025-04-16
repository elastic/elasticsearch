/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.search.aggregations.metrics;

import org.elasticsearch.common.geo.SpatialPoint;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.aggregations.metrics.InternalCentroid;
import org.elasticsearch.xpack.spatial.common.CartesianPoint;

import java.io.IOException;
import java.util.Map;

/**
 * Serialization and merge logic for {@link CartesianCentroidAggregator}.
 */
public class InternalCartesianCentroid extends InternalCentroid implements CartesianCentroid {

    public InternalCartesianCentroid(String name, SpatialPoint centroid, long count, Map<String, Object> metadata) {
        super(name, centroid, count, metadata);
    }

    /**
     * Read from a stream.
     */
    public InternalCartesianCentroid(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    protected CartesianPoint centroidFromStream(StreamInput in) throws IOException {
        return new CartesianPoint(in.readDouble(), in.readDouble());
    }

    static InternalCartesianCentroid empty(String name, Map<String, Object> metadata) {
        return new InternalCartesianCentroid(name, null, 0L, metadata);
    }

    @Override
    protected void centroidToStream(StreamOutput out) throws IOException {
        out.writeDouble(centroid.getX());
        out.writeDouble(centroid.getY());
    }

    @Override
    public String getWriteableName() {
        return CartesianCentroidAggregationBuilder.NAME;
    }

    @Override
    protected double extractDouble(String name) {
        return switch (name) {
            case "x" -> centroid.getX();
            case "y" -> centroid.getY();
            default -> throw new IllegalArgumentException("Found unknown path element [" + name + "] in [" + getName() + "]");
        };
    }

    @Override
    protected InternalCartesianCentroid copyWith(SpatialPoint result, long count) {
        return new InternalCartesianCentroid(name, result, count, getMetadata());
    }

    @Override
    protected InternalCartesianCentroid copyWith(double firstSum, double secondSum, long totalCount) {
        final CartesianPoint result = (Double.isNaN(firstSum)) ? null : new CartesianPoint(firstSum / totalCount, secondSum / totalCount);
        return copyWith(result, totalCount);
    }

    @Override
    protected String nameFirst() {
        return "x";
    }

    @Override
    protected double extractFirst(SpatialPoint point) {
        return point.getX();
    }

    @Override
    protected String nameSecond() {
        return "y";
    }

    @Override
    protected double extractSecond(SpatialPoint point) {
        return point.getY();
    }
}

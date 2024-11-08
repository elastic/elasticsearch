/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.common.geo.SpatialPoint;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.aggregations.AggregationReduceContext;
import org.elasticsearch.search.aggregations.AggregatorReducer;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.support.SamplingContext;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Serialization and merge logic for {@link GeoCentroidAggregator}.
 */
public abstract class InternalCentroid extends InternalAggregation implements CentroidAggregation {
    protected final SpatialPoint centroid;
    protected final long count;

    public InternalCentroid(String name, SpatialPoint centroid, long count, Map<String, Object> metadata) {
        super(name, metadata);
        assert (centroid == null) == (count == 0);
        this.centroid = centroid;
        assert count >= 0;
        this.count = count;
    }

    protected abstract SpatialPoint centroidFromStream(StreamInput in) throws IOException;

    protected abstract void centroidToStream(StreamOutput out) throws IOException;

    /**
     * Read from a stream.
     */
    @SuppressWarnings("this-escape")
    protected InternalCentroid(StreamInput in) throws IOException {
        super(in);
        count = in.readVLong();
        if (in.readBoolean()) {
            centroid = centroidFromStream(in);
        } else {
            centroid = null;
        }
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeVLong(count);
        if (centroid != null) {
            out.writeBoolean(true);
            centroidToStream(out);
        } else {
            out.writeBoolean(false);
        }
    }

    @Override
    public SpatialPoint centroid() {
        return centroid;
    }

    @Override
    public long count() {
        return count;
    }

    protected abstract InternalCentroid copyWith(SpatialPoint result, long count);

    /** Create a new centroid with by reducing from the sums and total count */
    protected abstract InternalCentroid copyWith(double firstSum, double secondSum, long totalCount);

    protected AggregatorReducer getLeaderReducer(AggregationReduceContext reduceContext, int size) {
        return new AggregatorReducer() {

            double firstSum = Double.NaN;
            double secondSum = Double.NaN;
            long totalCount = 0;

            @Override
            public void accept(InternalAggregation aggregation) {
                InternalCentroid centroidAgg = (InternalCentroid) aggregation;
                if (centroidAgg.count > 0) {
                    totalCount += centroidAgg.count;
                    if (Double.isNaN(firstSum)) {
                        firstSum = centroidAgg.count * extractFirst(centroidAgg.centroid);
                        secondSum = centroidAgg.count * extractSecond(centroidAgg.centroid);
                    } else {
                        firstSum += centroidAgg.count * extractFirst(centroidAgg.centroid);
                        secondSum += centroidAgg.count * extractSecond(centroidAgg.centroid);
                    }
                }
            }

            @Override
            public InternalAggregation get() {
                return copyWith(firstSum, secondSum, totalCount);
            }
        };
    }

    protected abstract String nameFirst();

    protected abstract double extractFirst(SpatialPoint point);

    protected abstract String nameSecond();

    protected abstract double extractSecond(SpatialPoint point);

    @Override
    public InternalAggregation finalizeSampling(SamplingContext samplingContext) {
        return copyWith(centroid, samplingContext.scaleUp(count));
    }

    @Override
    protected boolean mustReduceOnSingleInternalAgg() {
        return false;
    }

    protected abstract double extractDouble(String name);

    @Override
    public Object getProperty(List<String> path) {
        if (path.isEmpty()) {
            return this;
        } else if (path.size() == 1) {
            String coordinate = path.get(0);
            return switch (coordinate) {
                case "value" -> centroid;
                case "count" -> count;
                default -> extractDouble(coordinate);
            };
        } else {
            throw new IllegalArgumentException("path not supported for [" + getName() + "]: " + path);
        }
    }

    public static class Fields {
        public static final ParseField CENTROID = new ParseField("location");
        public static final ParseField COUNT = new ParseField("count");
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        if (centroid != null) {
            builder.startObject(Fields.CENTROID.getPreferredName());
            {
                builder.field(nameFirst(), extractFirst(centroid));
                builder.field(nameSecond(), extractSecond(centroid));
            }
            builder.endObject();
        }
        builder.field(Fields.COUNT.getPreferredName(), count);
        return builder;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;
        InternalCentroid that = (InternalCentroid) obj;
        return count == that.count && Objects.equals(centroid, that.centroid);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), centroid, count);
    }

    @Override
    public String toString() {
        return "InternalCentroid{" + "centroid=" + centroid + ", count=" + count + '}';
    }
}

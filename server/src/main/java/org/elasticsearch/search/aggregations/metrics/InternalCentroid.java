/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.common.geo.SpatialPoint;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.aggregations.AggregationReduceContext;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.support.SamplingContext;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

/**
 * Serialization and merge logic for {@link GeoCentroidAggregator}.
 */
public abstract class InternalCentroid extends InternalAggregation implements CentroidAggregation {
    protected final SpatialPoint centroid;
    protected final long count;
    private final FieldExtractor firstField;
    private final FieldExtractor secondField;

    public InternalCentroid(
        String name,
        SpatialPoint centroid,
        long count,
        Map<String, Object> metadata,
        FieldExtractor firstField,
        FieldExtractor secondField
    ) {
        super(name, metadata);
        assert (centroid == null) == (count == 0);
        this.centroid = centroid;
        assert count >= 0;
        this.count = count;
        this.firstField = firstField;
        this.secondField = secondField;
    }

    protected abstract SpatialPoint centroidFromStream(StreamInput in) throws IOException;

    protected abstract void centroidToStream(StreamOutput out) throws IOException;

    /**
     * Read from a stream.
     */
    protected InternalCentroid(StreamInput in, FieldExtractor firstField, FieldExtractor secondField) throws IOException {
        super(in);
        count = in.readVLong();
        if (in.readBoolean()) {
            centroid = centroidFromStream(in);
        } else {
            centroid = null;
        }
        this.firstField = firstField;
        this.secondField = secondField;
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

    @Override
    public InternalCentroid reduce(List<InternalAggregation> aggregations, AggregationReduceContext reduceContext) {
        double firstSum = Double.NaN;
        double secondSum = Double.NaN;
        long totalCount = 0;
        for (InternalAggregation aggregation : aggregations) {
            InternalCentroid centroidAgg = (InternalCentroid) aggregation;
            if (centroidAgg.count > 0) {
                totalCount += centroidAgg.count;
                if (Double.isNaN(firstSum)) {
                    firstSum = centroidAgg.count * firstField.extractor.apply(centroidAgg.centroid);
                    secondSum = centroidAgg.count * secondField.extractor.apply(centroidAgg.centroid);
                } else {
                    firstSum += centroidAgg.count * firstField.extractor.apply(centroidAgg.centroid);
                    secondSum += centroidAgg.count * secondField.extractor.apply(centroidAgg.centroid);
                }
            }
        }
        return copyWith(firstSum, secondSum, totalCount);
    }

    @Override
    public InternalAggregation finalizeSampling(SamplingContext samplingContext) {
        return copyWith(centroid, samplingContext.scaleUp(count));
    }

    @Override
    protected boolean mustReduceOnSingleInternalAgg() {
        return false;
    }

    protected static class FieldExtractor {
        private final String name;
        private final Function<SpatialPoint, Double> extractor;

        public FieldExtractor(String name, Function<SpatialPoint, Double> extractor) {
            this.name = name;
            this.extractor = extractor;
        }
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

    protected static class Fields {
        static final ParseField CENTROID = new ParseField("location");
        static final ParseField COUNT = new ParseField("count");
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        if (centroid != null) {
            builder.startObject(Fields.CENTROID.getPreferredName());
            {
                builder.field(firstField.name, firstField.extractor.apply(centroid));
                builder.field(secondField.name, secondField.extractor.apply(centroid));
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

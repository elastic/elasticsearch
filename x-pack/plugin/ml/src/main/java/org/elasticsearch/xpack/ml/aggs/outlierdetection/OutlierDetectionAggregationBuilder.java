/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.outlierdetection;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

public class OutlierDetectionAggregationBuilder extends AbstractAggregationBuilder<OutlierDetectionAggregationBuilder> {

    public static final String NAME = "outlier_detection";

    public static final ParseField FIELD_FIELD = new ParseField("field");
    public static final ParseField TOP_N_FIELD = new ParseField("top_n");
    public static final ParseField N_NEIGHBORS_FIELD = new ParseField("n_neighbors");
    public static final ParseField SAMPLE_SIZE_FIELD = new ParseField("sample_size");
    public static final ParseField PROJECTION_DIM_FIELD = new ParseField("projection_dim");
    public static final ParseField SEED_FIELD = new ParseField("seed");
    public static final ParseField OVERFETCH_FACTOR_FIELD = new ParseField("overfetch_factor");
    public static final ParseField METHOD_FIELD = new ParseField("method");
    public static final ParseField MAX_COORD_SAMPLE_FIELD = new ParseField("max_coord_sample");
    public static final ParseField NORMALIZE_FIELD = new ParseField("normalize");

    public static final int DEFAULT_TOP_N = 10;
    public static final int DEFAULT_N_NEIGHBORS = 5;
    public static final int DEFAULT_OVERFETCH_FACTOR = 3;
    public static final int DEFAULT_MAX_COORD_SAMPLE = 500;

    public static final ObjectParser<OutlierDetectionAggregationBuilder, String> PARSER = ObjectParser.fromBuilder(
        NAME,
        OutlierDetectionAggregationBuilder::new
    );

    static {
        PARSER.declareString(OutlierDetectionAggregationBuilder::setField, FIELD_FIELD);
        PARSER.declareInt(OutlierDetectionAggregationBuilder::setTopN, TOP_N_FIELD);
        PARSER.declareInt(OutlierDetectionAggregationBuilder::setNNeighbors, N_NEIGHBORS_FIELD);
        PARSER.declareInt(OutlierDetectionAggregationBuilder::setSampleSize, SAMPLE_SIZE_FIELD);
        PARSER.declareInt(OutlierDetectionAggregationBuilder::setProjectionDim, PROJECTION_DIM_FIELD);
        PARSER.declareLong(OutlierDetectionAggregationBuilder::setSeed, SEED_FIELD);
        PARSER.declareInt(OutlierDetectionAggregationBuilder::setOverfetchFactor, OVERFETCH_FACTOR_FIELD);
        PARSER.declareString(OutlierDetectionAggregationBuilder::setMethod, METHOD_FIELD);
        PARSER.declareInt(OutlierDetectionAggregationBuilder::setMaxCoordSample, MAX_COORD_SAMPLE_FIELD);
        PARSER.declareString(OutlierDetectionAggregationBuilder::setNormalize, NORMALIZE_FIELD);
    }

    private String field;
    private int topN = DEFAULT_TOP_N;
    private int nNeighbors = DEFAULT_N_NEIGHBORS;
    private Integer sampleSize;
    private Integer projectionDim;
    private Long seed;
    private int overfetchFactor = DEFAULT_OVERFETCH_FACTOR;
    private OutlierDetectionMethod method = OutlierDetectionMethod.DEFAULT;
    private int maxCoordSample = DEFAULT_MAX_COORD_SAMPLE;
    private ScoreNormalization normalize = ScoreNormalization.NONE;

    public OutlierDetectionAggregationBuilder(String name) {
        super(name);
    }

    protected OutlierDetectionAggregationBuilder(
        OutlierDetectionAggregationBuilder clone,
        AggregatorFactories.Builder factoriesBuilder,
        Map<String, Object> metadata
    ) {
        super(clone, factoriesBuilder, metadata);
        this.field = clone.field;
        this.topN = clone.topN;
        this.nNeighbors = clone.nNeighbors;
        this.sampleSize = clone.sampleSize;
        this.projectionDim = clone.projectionDim;
        this.seed = clone.seed;
        this.overfetchFactor = clone.overfetchFactor;
        this.method = clone.method;
        this.maxCoordSample = clone.maxCoordSample;
        this.normalize = clone.normalize;
    }

    public OutlierDetectionAggregationBuilder(StreamInput in) throws IOException {
        super(in);
        this.field = in.readString();
        this.topN = in.readVInt();
        this.nNeighbors = in.readVInt();
        this.sampleSize = in.readOptionalVInt();
        this.projectionDim = in.readOptionalVInt();
        this.seed = in.readOptionalLong();
        this.overfetchFactor = in.readVInt();
        this.method = in.readEnum(OutlierDetectionMethod.class);
        this.maxCoordSample = in.readVInt();
        this.normalize = in.readEnum(ScoreNormalization.class);
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(field);
        out.writeVInt(topN);
        out.writeVInt(nNeighbors);
        out.writeOptionalVInt(sampleSize);
        out.writeOptionalVInt(projectionDim);
        out.writeOptionalLong(seed);
        out.writeVInt(overfetchFactor);
        out.writeEnum(method);
        out.writeVInt(maxCoordSample);
        out.writeEnum(normalize);
    }

    @Override
    protected AggregationBuilder shallowCopy(AggregatorFactories.Builder factoriesBuilder, Map<String, Object> metadata) {
        return new OutlierDetectionAggregationBuilder(this, factoriesBuilder, metadata);
    }

    @Override
    public String getType() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersion.current();
    }

    @Override
    protected AggregatorFactory doBuild(
        AggregationContext context,
        AggregatorFactory parent,
        AggregatorFactories.Builder subFactoriesBuilder
    ) throws IOException {
        if (field == null || field.isEmpty()) {
            throw new IllegalArgumentException("[field] is required for [" + NAME + "] aggregation");
        }
        return new OutlierDetectionAggregatorFactory(
            name,
            context,
            parent,
            subFactoriesBuilder,
            metadata,
            field,
            topN,
            nNeighbors,
            sampleSize,
            projectionDim,
            seed,
            overfetchFactor,
            method,
            maxCoordSample,
            normalize
        );
    }

    @Override
    protected XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(FIELD_FIELD.getPreferredName(), field);
        builder.field(TOP_N_FIELD.getPreferredName(), topN);
        builder.field(N_NEIGHBORS_FIELD.getPreferredName(), nNeighbors);
        if (sampleSize != null) {
            builder.field(SAMPLE_SIZE_FIELD.getPreferredName(), sampleSize);
        }
        if (projectionDim != null) {
            builder.field(PROJECTION_DIM_FIELD.getPreferredName(), projectionDim);
        }
        if (seed != null) {
            builder.field(SEED_FIELD.getPreferredName(), seed);
        }
        builder.field(OVERFETCH_FACTOR_FIELD.getPreferredName(), overfetchFactor);
        builder.field(METHOD_FIELD.getPreferredName(), method.toString());
        builder.field(MAX_COORD_SAMPLE_FIELD.getPreferredName(), maxCoordSample);
        builder.field(NORMALIZE_FIELD.getPreferredName(), normalize.toString());
        builder.endObject();
        return builder;
    }

    @Override
    public BucketCardinality bucketCardinality() {
        return BucketCardinality.NONE;
    }

    // Setters

    public OutlierDetectionAggregationBuilder setField(String field) {
        this.field = field;
        return this;
    }

    public OutlierDetectionAggregationBuilder setTopN(int topN) {
        if (topN < 1) {
            throw new IllegalArgumentException("[top_n] must be >= 1, got [" + topN + "]");
        }
        this.topN = topN;
        return this;
    }

    public OutlierDetectionAggregationBuilder setNNeighbors(int nNeighbors) {
        if (nNeighbors < 1) {
            throw new IllegalArgumentException("[n_neighbors] must be >= 1, got [" + nNeighbors + "]");
        }
        this.nNeighbors = nNeighbors;
        return this;
    }

    public OutlierDetectionAggregationBuilder setSampleSize(int sampleSize) {
        if (sampleSize < 1) {
            throw new IllegalArgumentException("[sample_size] must be >= 1, got [" + sampleSize + "]");
        }
        this.sampleSize = sampleSize;
        return this;
    }

    public OutlierDetectionAggregationBuilder setProjectionDim(int projectionDim) {
        if (projectionDim < 1) {
            throw new IllegalArgumentException("[projection_dim] must be >= 1, got [" + projectionDim + "]");
        }
        this.projectionDim = projectionDim;
        return this;
    }

    public OutlierDetectionAggregationBuilder setSeed(long seed) {
        this.seed = seed;
        return this;
    }

    public OutlierDetectionAggregationBuilder setOverfetchFactor(int overfetchFactor) {
        if (overfetchFactor < 1) {
            throw new IllegalArgumentException("[overfetch_factor] must be >= 1, got [" + overfetchFactor + "]");
        }
        this.overfetchFactor = overfetchFactor;
        return this;
    }

    public OutlierDetectionAggregationBuilder setMethod(String method) {
        this.method = OutlierDetectionMethod.fromString(method);
        return this;
    }

    public OutlierDetectionAggregationBuilder setMethod(OutlierDetectionMethod method) {
        this.method = method;
        return this;
    }

    public OutlierDetectionAggregationBuilder setMaxCoordSample(int maxCoordSample) {
        if (maxCoordSample < 1) {
            throw new IllegalArgumentException("[max_coord_sample] must be >= 1, got [" + maxCoordSample + "]");
        }
        this.maxCoordSample = maxCoordSample;
        return this;
    }

    public OutlierDetectionAggregationBuilder setNormalize(String normalize) {
        this.normalize = ScoreNormalization.fromString(normalize);
        return this;
    }

    public OutlierDetectionAggregationBuilder setNormalize(ScoreNormalization normalize) {
        this.normalize = normalize;
        return this;
    }

    // Getters

    public String getField() {
        return field;
    }

    public int getTopN() {
        return topN;
    }

    public int getNNeighbors() {
        return nNeighbors;
    }

    public Integer getSampleSize() {
        return sampleSize;
    }

    public Integer getProjectionDim() {
        return projectionDim;
    }

    public Long getSeed() {
        return seed;
    }

    public int getOverfetchFactor() {
        return overfetchFactor;
    }

    public OutlierDetectionMethod getMethod() {
        return method;
    }

    public int getMaxCoordSample() {
        return maxCoordSample;
    }

    public ScoreNormalization getNormalize() {
        return normalize;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), field, topN, nNeighbors, sampleSize, projectionDim, seed, overfetchFactor, method, maxCoordSample, normalize);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;
        OutlierDetectionAggregationBuilder other = (OutlierDetectionAggregationBuilder) obj;
        return Objects.equals(field, other.field)
            && topN == other.topN
            && nNeighbors == other.nNeighbors
            && Objects.equals(sampleSize, other.sampleSize)
            && Objects.equals(projectionDim, other.projectionDim)
            && Objects.equals(seed, other.seed)
            && overfetchFactor == other.overfetchFactor
            && method == other.method
            && maxCoordSample == other.maxCoordSample
            && normalize == other.normalize;
    }
}

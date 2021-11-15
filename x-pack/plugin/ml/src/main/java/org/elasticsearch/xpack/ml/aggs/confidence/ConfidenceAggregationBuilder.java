/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.confidence;

import com.carrotsearch.hppc.BitMixer;

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
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;

public class ConfidenceAggregationBuilder extends AbstractAggregationBuilder<ConfidenceAggregationBuilder> {

    public static final String NAME = "confidence";

    static final ParseField CONFIDENCE_INTERVAL = new ParseField("confidence_interval");
    static final ParseField PROBABILITY = new ParseField("p");
    static final ParseField SEED = new ParseField("seed");
    static final ParseField KEYED = new ParseField("keyed");
    static final ParseField METRICS = new ParseField("metrics");

    public static final ObjectParser<ConfidenceAggregationBuilder, String> PARSER = ObjectParser.fromBuilder(
        ConfidenceAggregationBuilder.NAME,
        ConfidenceAggregationBuilder::new
    );
    static {
        PARSER.declareInt(ConfidenceAggregationBuilder::setConfidenceInterval, CONFIDENCE_INTERVAL);
        PARSER.declareInt(ConfidenceAggregationBuilder::setSeed, SEED);
        PARSER.declareDouble(ConfidenceAggregationBuilder::setProbability, PROBABILITY);
        PARSER.declareObject(ConfidenceAggregationBuilder::setMetrics, (p, c) -> p.mapStrings(), METRICS);
        PARSER.declareBoolean(ConfidenceAggregationBuilder::setKeyed, KEYED);
    }

    public static ConfidenceAggregationBuilder parse(String aggregationName, XContentParser parser) throws IOException {
        return PARSER.parse(parser, new ConfidenceAggregationBuilder(aggregationName), null);
    }

    private int confidenceInterval = 20;
    private Integer seed;
    private double p = 0.1;
    private Map<String, String> metrics;
    private boolean keyed;

    private ConfidenceAggregationBuilder(String name) {
        super(name);
    }

    public ConfidenceAggregationBuilder setConfidenceInterval(int confidenceInterval) {
        if (confidenceInterval <= 0 || confidenceInterval >= 100) {
            throw ExceptionsHelper.badRequestException(
                "[{}] must be greater than 0 and less than 100. Found [{}] in [{}]",
                CONFIDENCE_INTERVAL.getPreferredName(),
                confidenceInterval,
                name
            );
        }
        this.confidenceInterval = confidenceInterval;
        return this;
    }

    public ConfidenceAggregationBuilder setProbability(double probability) {
        if (probability <= 0 || probability >= 1) {
            throw new IllegalArgumentException("no");
        }
        this.p = probability;
        return this;
    }

    public ConfidenceAggregationBuilder setMetrics(Map<String, String> metrics) {
        if (metrics.isEmpty()) {
            throw new IllegalArgumentException("no");
        }
        this.metrics = metrics;
        return this;
    }

    public ConfidenceAggregationBuilder setSeed(int seed) {
        this.seed = seed;
        return this;
    }

    public ConfidenceAggregationBuilder setKeyed(boolean keyed) {
        this.keyed = keyed;
        return this;
    }

    public ConfidenceAggregationBuilder(StreamInput in) throws IOException {
        super(in);
        this.confidenceInterval = in.readVInt();
        this.p = in.readDouble();
        this.seed = in.readOptionalInt();
        this.metrics = in.readMap(StreamInput::readString, StreamInput::readString);
        this.keyed = in.readBoolean();
    }

    protected ConfidenceAggregationBuilder(
        ConfidenceAggregationBuilder clone,
        AggregatorFactories.Builder factoriesBuilder,
        Map<String, Object> metadata
    ) {
        super(clone, factoriesBuilder, metadata);
        this.confidenceInterval = clone.confidenceInterval;
        this.p = clone.p;
        this.seed = clone.seed;
        this.metrics = clone.metrics;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeVInt(confidenceInterval);
        out.writeDouble(p);
        out.writeOptionalInt(seed);
        out.writeMap(metrics, StreamOutput::writeString, StreamOutput::writeString);
        out.writeBoolean(keyed);
    }

    @Override
    protected AggregatorFactory doBuild(
        AggregationContext context,
        AggregatorFactory parent,
        AggregatorFactories.Builder subfactoriesBuilder
    ) throws IOException {
        return new ConfidenceAggregatorFactory(
            name,
            confidenceInterval,
            p,
            Optional.ofNullable(seed).orElseGet(() -> BitMixer.mix(context.nowInMillis())),
            keyed,
            metrics,
            context,
            parent,
            subfactoriesBuilder,
            metadata
        );
    }

    @Override
    protected XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(CONFIDENCE_INTERVAL.getPreferredName(), confidenceInterval);
        builder.field(PROBABILITY.getPreferredName(), p);
        builder.field(SEED.getPreferredName(), seed);
        builder.field(METRICS.getPreferredName(), metrics);
        builder.endObject();
        return null;
    }

    @Override
    protected AggregationBuilder shallowCopy(AggregatorFactories.Builder factoriesBuilder, Map<String, Object> metadata) {
        return new ConfidenceAggregationBuilder(this, factoriesBuilder, metadata);
    }

    @Override
    public BucketCardinality bucketCardinality() {
        return BucketCardinality.MANY;
    }

    @Override
    public String getType() {
        return NAME;
    }
}

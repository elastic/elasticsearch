/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.confidence;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ValidateActions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.bucket.filter.FilterAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.filter.FiltersAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.HistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.range.AbstractRangeBuilder;
import org.elasticsearch.search.aggregations.bucket.sampler.random.RandomSamplerAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.sampler.random.RandomSamplerAggregatorFactory;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.PercentilesAggregationBuilder;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregationBuilder;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

public class ConfidenceAggregationBuilder extends AbstractAggregationBuilder<ConfidenceAggregationBuilder> {

    public static final String NAME = "confidence";

    static final ParseField CONFIDENCE_INTERVAL = new ParseField("confidence_interval");
    static final ParseField KEYED = new ParseField("keyed");
    static final ParseField DEBUG = new ParseField("debug");

    public static final ObjectParser<ConfidenceAggregationBuilder, String> PARSER = ObjectParser.fromBuilder(
        ConfidenceAggregationBuilder.NAME,
        ConfidenceAggregationBuilder::new
    );
    static {
        PARSER.declareInt(ConfidenceAggregationBuilder::setConfidenceInterval, CONFIDENCE_INTERVAL);
        PARSER.declareBoolean(ConfidenceAggregationBuilder::setKeyed, KEYED);
        PARSER.declareBoolean(ConfidenceAggregationBuilder::setDebug, DEBUG);
    }

    public static ConfidenceAggregationBuilder parse(String aggregationName, XContentParser parser) throws IOException {
        return PARSER.parse(parser, new ConfidenceAggregationBuilder(aggregationName), null);
    }

    private int confidenceInterval = 20;
    private boolean keyed;
    private boolean debug;

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

    @Override
    public boolean supportsSampling() {
        return true;
    }

    public ConfidenceAggregationBuilder setKeyed(boolean keyed) {
        this.keyed = keyed;
        return this;
    }

    public ConfidenceAggregationBuilder setDebug(boolean debug) {
        this.debug = debug;
        return this;
    }

    public ConfidenceAggregationBuilder(StreamInput in) throws IOException {
        super(in);
        this.confidenceInterval = in.readVInt();
        this.keyed = in.readBoolean();
        this.debug = in.readBoolean();
    }

    protected ConfidenceAggregationBuilder(
        ConfidenceAggregationBuilder clone,
        AggregatorFactories.Builder factoriesBuilder,
        Map<String, Object> metadata
    ) {
        super(clone, factoriesBuilder, metadata);
        this.confidenceInterval = clone.confidenceInterval;
        this.keyed = clone.keyed;
        this.debug = clone.debug;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeVInt(confidenceInterval);
        out.writeBoolean(keyed);
        out.writeBoolean(debug);
    }

    static void recursivelyCheckSubAggs(Collection<AggregationBuilder> builders, Consumer<AggregationBuilder> aggregationCheck) {
        if (builders == null || builders.isEmpty()) {
            return;
        }
        for (AggregationBuilder b : builders) {
            aggregationCheck.accept(b);
            recursivelyCheckSubAggs(b.getSubAggregations(), aggregationCheck);
        }
    }

    @Override
    protected AggregatorFactory doBuild(
        AggregationContext context,
        AggregatorFactory parent,
        AggregatorFactories.Builder subfactoriesBuilder
    ) throws IOException {
        ActionRequestValidationException validationException = null;
        if (parent instanceof RandomSamplerAggregatorFactory == false) {
            validationException = ValidateActions.addValidationError(
                "parent aggregation must be [" + RandomSamplerAggregationBuilder.NAME + "]",
                validationException
            );
        }
        Set<String> previousAggNames = new HashSet<>();
        previousAggNames.add(getName());
        recursivelyCheckSubAggs(subfactoriesBuilder.getAggregatorFactories(), builder -> {
            if (previousAggNames.add(builder.getName()) == false) {
                throw new IllegalArgumentException(
                    "[confidence] aggregation requires all sub-aggs have unique names; found duplicate name[" + builder.getName() + "]"
                );
            }
            if (builder instanceof ValuesSourceAggregationBuilder.SingleMetricAggregationBuilder
                || builder instanceof PercentilesAggregationBuilder
                || builder instanceof TermsAggregationBuilder
                || builder instanceof AbstractRangeBuilder
                || builder instanceof DateHistogramAggregationBuilder
                || builder instanceof HistogramAggregationBuilder
                || builder instanceof FilterAggregationBuilder
                || builder instanceof FiltersAggregationBuilder) if (builder.supportsSampling() == false) {
                    throw new IllegalArgumentException(
                        "[random_sampler] aggregation ["
                            + getName()
                            + "] does not support sampling ["
                            + builder.getType()
                            + "] aggregation ["
                            + builder.getName()
                            + "]"
                    );
                }
        });

        if (validationException != null) {
            throw validationException;
        }
        return new ConfidenceAggregatorFactory(name, confidenceInterval, keyed, debug, context, parent, subfactoriesBuilder, metadata);
    }

    @Override
    protected XContentBuilder internalXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject();
        builder.field(CONFIDENCE_INTERVAL.getPreferredName(), confidenceInterval);
        builder.field(KEYED.getPreferredName(), keyed);
        builder.field(DEBUG.getPreferredName(), debug);
        builder.endObject();
        return null;
    }

    @Override
    protected AggregationBuilder shallowCopy(AggregatorFactories.Builder factoriesBuilder, Map<String, Object> metadata) {
        return new ConfidenceAggregationBuilder(this, factoriesBuilder, metadata);
    }

    @Override
    public AggregationBuilder.BucketCardinality bucketCardinality() {
        return AggregationBuilder.BucketCardinality.MANY;
    }

    @Override
    public String getType() {
        return NAME;
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return Version.V_8_3_0;
    }
}

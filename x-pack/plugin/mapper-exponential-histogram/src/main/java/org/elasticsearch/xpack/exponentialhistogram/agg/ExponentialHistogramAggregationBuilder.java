/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.xpack.exponentialhistogram.agg;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.MultiBucketConsumerService;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceRegistry;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.exponentialhistogram.ExponentialHistogramValuesSourceType;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

public class ExponentialHistogramAggregationBuilder extends ValuesSourceAggregationBuilder<ExponentialHistogramAggregationBuilder> {

    public static final String NAME = "exponential_histogram";
    public static final ValuesSourceRegistry.RegistryKey<ExponentialHistogramAggregatorSupplier> REGISTRY_KEY =
        new ValuesSourceRegistry.RegistryKey<>(NAME, ExponentialHistogramAggregatorSupplier.class);

    private static final ParseField MAX_BUCKETS_FIELD = new ParseField("max_buckets");

    private static final ParseField MAX_SCALE_FIELD = new ParseField("max_scale");

    public static final ObjectParser<ExponentialHistogramAggregationBuilder, String> PARSER = ObjectParser.fromBuilder(
        NAME,
        ExponentialHistogramAggregationBuilder::new
    );
    static {
        ValuesSourceAggregationBuilder.declareFields(PARSER, true, true, true);
        PARSER.declareInt(ExponentialHistogramAggregationBuilder::setMaxBuckets, MAX_BUCKETS_FIELD);
        PARSER.declareInt(ExponentialHistogramAggregationBuilder::setMaxScale, MAX_SCALE_FIELD);
    }

    private int maxBuckets = 160;
    private int maxScale = 20;

    /** Create a new builder with the given name. */
    public ExponentialHistogramAggregationBuilder(String name) {
        super(name);
    }

    /** Read in object data from a stream, for internal use only. */
    public ExponentialHistogramAggregationBuilder(StreamInput in) throws IOException {
        super(in);
        maxBuckets = in.readVInt();
        maxScale = in.readVInt();
    }

    protected ExponentialHistogramAggregationBuilder(
        ExponentialHistogramAggregationBuilder clone,
        AggregatorFactories.Builder factoriesBuilder,
        Map<String, Object> metaData
    ) {
        super(clone, factoriesBuilder, metaData);
        this.maxBuckets = clone.maxBuckets;
        this.maxScale = clone.maxScale;
    }

    @Override
    protected ValuesSourceType defaultValueSourceType() {
        return ExponentialHistogramValuesSourceType.HISTOGRAM;
    }

    public ExponentialHistogramAggregationBuilder setMaxBuckets(int maxBuckets) {
        if (maxBuckets <= 1) {
            throw new IllegalArgumentException(MAX_BUCKETS_FIELD.getPreferredName() + " must be greater than [1] for [" + name + "]");
        }
        this.maxBuckets = maxBuckets;
        return this;
    }

    public ExponentialHistogramAggregationBuilder setMaxScale(int maxScale) {
        if (maxScale <= 1) {
            // A shard size of 1 will cause divide by 0s and, even if it worked, would produce garbage results.
            throw new IllegalArgumentException(MAX_SCALE_FIELD.getPreferredName() + " must be greater than [1] for [" + name + "]");
        }
        this.maxScale = maxScale;
        return this;
    }

    @Override
    public BucketCardinality bucketCardinality() {
        return BucketCardinality.ONE;
    }

    @Override
    protected AggregationBuilder shallowCopy(AggregatorFactories.Builder factoriesBuilder, Map<String, Object> metaData) {
        return new ExponentialHistogramAggregationBuilder(this, factoriesBuilder, metaData);
    }

    @Override
    protected void innerWriteTo(StreamOutput out) throws IOException {
        out.writeVInt(maxBuckets);
        out.writeVInt(maxScale);
    }

    @Override
    protected ValuesSourceAggregatorFactory innerBuild(
        AggregationContext context,
        ValuesSourceConfig config,
        AggregatorFactory parent,
        AggregatorFactories.Builder subFactoriesBuilder
    ) throws IOException {
        Settings settings = context.getIndexSettings().getNodeSettings();
        int maxBucketsSetting = MultiBucketConsumerService.MAX_BUCKET_SETTING.get(settings);
        if (maxBuckets > maxBucketsSetting) {
            throw new IllegalArgumentException(MAX_BUCKETS_FIELD.getPreferredName() + " must be less than " + maxBucketsSetting);
        }

        ExponentialHistogramAggregatorSupplier aggregatorSupplier = context.getValuesSourceRegistry().getAggregator(REGISTRY_KEY, config);
        return new ExponentialHistogramAggregatorFactory(
            name,
            config,
            maxBuckets,
            maxScale,
            context,
            parent,
            subFactoriesBuilder,
            metadata,
            aggregatorSupplier
        );
    }

    @Override
    protected XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.field(MAX_BUCKETS_FIELD.getPreferredName(), maxBuckets);
        builder.field(MAX_SCALE_FIELD.getPreferredName(), maxScale);
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), maxBuckets, maxScale);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;
        ExponentialHistogramAggregationBuilder other = (ExponentialHistogramAggregationBuilder) obj;
        return Objects.equals(maxBuckets, other.maxBuckets)
            && Objects.equals(maxScale, other.maxScale);
    }

    @Override
    public String getType() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.V_7_9_0;
    }
}

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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class ExponentialHistogramPercentilesAggregationBuilder
    extends ValuesSourceAggregationBuilder.MetricsAggregationBuilder<ExponentialHistogramPercentilesAggregationBuilder> {

    public static final String NAME = "exponential_histogram_percentiles";
    public static final ValuesSourceRegistry.RegistryKey<ExponentialHistogramPercentilesAggregatorSupplier> REGISTRY_KEY =
        new ValuesSourceRegistry.RegistryKey<>(NAME, ExponentialHistogramPercentilesAggregatorSupplier.class);

    private static final ParseField MAX_BUCKETS_FIELD = new ParseField("max_buckets");

    private static final ParseField MAX_SCALE_FIELD = new ParseField("max_scale");
    private static final ParseField VALUES_FIELD = new ParseField("values");

    private static final double[] DEFAULT_PERCENTILES = new double[] { 1, 5, 25, 50, 75, 95, 99 };

    public static final ObjectParser<ExponentialHistogramPercentilesAggregationBuilder, String> PARSER = ObjectParser.fromBuilder(
        NAME,
        ExponentialHistogramPercentilesAggregationBuilder::new
    );
    static {
        ValuesSourceAggregationBuilder.declareFields(PARSER, true, true, true);
        PARSER.declareInt(ExponentialHistogramPercentilesAggregationBuilder::setMaxBuckets, MAX_BUCKETS_FIELD);
        PARSER.declareInt(ExponentialHistogramPercentilesAggregationBuilder::setMaxScale, MAX_SCALE_FIELD);
        PARSER.declareDoubleArray(ExponentialHistogramPercentilesAggregationBuilder::setValues, VALUES_FIELD);
    }

    private int maxBuckets = 160;
    private int maxScale = 20;
    private double[] values = DEFAULT_PERCENTILES;

    /** Create a new builder with the given name. */
    public ExponentialHistogramPercentilesAggregationBuilder(String name) {
        super(name);
    }

    /** Read in object data from a stream, for internal use only. */
    public ExponentialHistogramPercentilesAggregationBuilder(StreamInput in) throws IOException {
        super(in);
        maxBuckets = in.readVInt();
        maxScale = in.readVInt();
        values = in.readDoubleArray();
    }

    protected ExponentialHistogramPercentilesAggregationBuilder(
        ExponentialHistogramPercentilesAggregationBuilder clone,
        AggregatorFactories.Builder factoriesBuilder,
        Map<String, Object> metaData
    ) {
        super(clone, factoriesBuilder, metaData);
        this.maxBuckets = clone.maxBuckets;
        this.maxScale = clone.maxScale;
        this.values = clone.values;
    }

    @Override
    public Set<String> metricNames() {
        return Arrays.stream(values).mapToObj(String::valueOf).collect(Collectors.toSet());
    }

    @Override
    protected ValuesSourceType defaultValueSourceType() {
        return ExponentialHistogramValuesSourceType.HISTOGRAM;
    }

    public ExponentialHistogramPercentilesAggregationBuilder setMaxBuckets(int maxBuckets) {
        if (maxBuckets <= 1) {
            throw new IllegalArgumentException(MAX_BUCKETS_FIELD.getPreferredName() + " must be greater than [1] for [" + name + "]");
        }
        this.maxBuckets = maxBuckets;
        return this;
    }

    public ExponentialHistogramPercentilesAggregationBuilder setMaxScale(int maxScale) {
        if (maxScale <= 1) {
            // A shard size of 1 will cause divide by 0s and, even if it worked, would produce garbage results.
            throw new IllegalArgumentException(MAX_SCALE_FIELD.getPreferredName() + " must be greater than [1] for [" + name + "]");
        }
        this.maxScale = maxScale;
        return this;
    }

    public ExponentialHistogramPercentilesAggregationBuilder setValues(List<Double> values) {
        this.values = values.stream().mapToDouble(Double::doubleValue).toArray();
        return this;
    }

    @Override
    protected AggregationBuilder shallowCopy(AggregatorFactories.Builder factoriesBuilder, Map<String, Object> metaData) {
        return new ExponentialHistogramPercentilesAggregationBuilder(this, factoriesBuilder, metaData);
    }

    @Override
    protected void innerWriteTo(StreamOutput out) throws IOException {
        out.writeVInt(maxBuckets);
        out.writeVInt(maxScale);
        out.writeDoubleArray(values);
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

        ExponentialHistogramPercentilesAggregatorSupplier aggregatorSupplier =
            context.getValuesSourceRegistry().getAggregator(REGISTRY_KEY, config);
        return new ExponentialHistogramPercentilesAggregatorFactory(
            name,
            config,
            maxBuckets,
            maxScale,
            values,
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
        builder.field(VALUES_FIELD.getPreferredName(), values);
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), maxBuckets, maxScale, values);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;
        ExponentialHistogramPercentilesAggregationBuilder other = (ExponentialHistogramPercentilesAggregationBuilder) obj;
        return Objects.equals(maxBuckets, other.maxBuckets)
            && Objects.equals(maxScale, other.maxScale)
            && Objects.equals(values, other.values);
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

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.analytics.rate;

import org.elasticsearch.Version;
import org.elasticsearch.common.Rounding;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceRegistry;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

public class RateAggregationBuilder extends ValuesSourceAggregationBuilder.LeafOnly<ValuesSource, RateAggregationBuilder> {
    public static final String NAME = "rate";
    public static final ParseField UNIT_FIELD = new ParseField("unit");
    public static final ParseField MODE_FIELD = new ParseField("mode");
    public static final ValuesSourceRegistry.RegistryKey<RateAggregatorSupplier> REGISTRY_KEY = new ValuesSourceRegistry.RegistryKey<>(
        NAME,
        RateAggregatorSupplier.class
    );
    public static final ObjectParser<RateAggregationBuilder, String> PARSER = ObjectParser.fromBuilder(NAME, RateAggregationBuilder::new);

    static {
        ValuesSourceAggregationBuilder.declareFields(PARSER, true, true, false, false);
        PARSER.declareString(RateAggregationBuilder::rateUnit, UNIT_FIELD);
        PARSER.declareString(RateAggregationBuilder::rateMode, MODE_FIELD);
    }

    Rounding.DateTimeUnit rateUnit;
    RateMode rateMode;

    public static void registerAggregators(ValuesSourceRegistry.Builder builder) {
        RateAggregatorFactory.registerAggregators(builder);
    }

    public RateAggregationBuilder(String name) {
        super(name);
    }

    protected RateAggregationBuilder(
        RateAggregationBuilder clone,
        AggregatorFactories.Builder factoriesBuilder,
        Map<String, Object> metadata
    ) {
        super(clone, factoriesBuilder, metadata);
        this.rateUnit = clone.rateUnit;
        this.rateMode = clone.rateMode;
    }

    @Override
    protected AggregationBuilder shallowCopy(AggregatorFactories.Builder factoriesBuilder, Map<String, Object> metadata) {
        return new RateAggregationBuilder(this, factoriesBuilder, metadata);
    }

    /**
     * Read from a stream.
     */
    public RateAggregationBuilder(StreamInput in) throws IOException {
        super(in);
        byte b = in.readByte();
        if (b > 0) {
            rateUnit = Rounding.DateTimeUnit.resolve(b);
        } else {
            rateUnit = null;
        }
        if (in.getVersion().onOrAfter(Version.V_7_11_0)) {
            if (in.readBoolean()) {
                rateMode = in.readEnum(RateMode.class);
            }
        }
    }

    @Override
    protected ValuesSourceType defaultValueSourceType() {
        return CoreValuesSourceType.NUMERIC;
    }

    @Override
    protected void innerWriteTo(StreamOutput out) throws IOException {
        if (rateUnit != null) {
            out.writeByte(rateUnit.getId());
        } else {
            out.writeByte((byte) 0);
        }
        if (out.getVersion().onOrAfter(Version.V_7_11_0)) {
            if (rateMode != null) {
                out.writeBoolean(true);
                out.writeEnum(rateMode);
            } else {
                out.writeBoolean(false);
            }
        }
    }

    @Override
    protected ValuesSourceRegistry.RegistryKey<?> getRegistryKey() {
        return REGISTRY_KEY;
    }

    @Override
    protected RateAggregatorFactory innerBuild(
        AggregationContext context,
        ValuesSourceConfig config,
        AggregatorFactory parent,
        AggregatorFactories.Builder subFactoriesBuilder
    ) throws IOException {
        if (field() == null && script() == null) {
            if (rateMode != null) {
                throw new IllegalArgumentException("The mode parameter is only supported with field or script");
            }
        }

        RateAggregatorSupplier aggregatorSupplier = context.getValuesSourceRegistry().getAggregator(REGISTRY_KEY, config);
        return new RateAggregatorFactory(
            name,
            config,
            rateUnit,
            rateMode,
            context,
            parent,
            subFactoriesBuilder,
            metadata,
            aggregatorSupplier
        );
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        if (rateUnit != null) {
            builder.field(UNIT_FIELD.getPreferredName(), rateUnit.shortName());
        }
        if (rateMode != null) {
            builder.field(MODE_FIELD.getPreferredName(), rateMode.value());
        }
        return builder;
    }

    @Override
    public String getType() {
        return NAME;
    }

    public RateAggregationBuilder rateUnit(String rateUnit) {
        return rateUnit(parse(rateUnit));
    }

    public RateAggregationBuilder rateUnit(Rounding.DateTimeUnit rateUnit) {
        this.rateUnit = rateUnit;
        return this;
    }

    public RateAggregationBuilder rateMode(String rateMode) {
        return rateMode(RateMode.resolve(rateMode));
    }

    public RateAggregationBuilder rateMode(RateMode rateMode) {
        this.rateMode = rateMode;
        return this;
    }

    static Rounding.DateTimeUnit parse(String rateUnit) {
        Rounding.DateTimeUnit parsedRate = DateHistogramAggregationBuilder.DATE_FIELD_UNITS.get(rateUnit);
        if (parsedRate == null) {
            throw new IllegalArgumentException("Unsupported unit " + rateUnit);
        }
        return parsedRate;
    }

    @Override
    protected ValuesSourceConfig resolveConfig(AggregationContext context) {
        if (field() == null && script() == null) {
            return new ValuesSourceConfig(CoreValuesSourceType.NUMERIC, null, true, null, null, 1.0, null, DocValueFormat.RAW, context);
        } else {
            return super.resolveConfig(context);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (super.equals(o) == false) return false;
        RateAggregationBuilder that = (RateAggregationBuilder) o;
        return rateUnit == that.rateUnit && rateMode == that.rateMode;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), rateUnit, rateMode);
    }
}

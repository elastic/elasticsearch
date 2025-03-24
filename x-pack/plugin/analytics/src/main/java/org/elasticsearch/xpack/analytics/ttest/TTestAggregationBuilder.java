/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.analytics.ttest;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.FieldContext;
import org.elasticsearch.search.aggregations.support.MultiValuesSourceAggregationBuilder;
import org.elasticsearch.search.aggregations.support.MultiValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.MultiValuesSourceFieldConfig;
import org.elasticsearch.search.aggregations.support.MultiValuesSourceParseHelper;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceRegistry;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

public class TTestAggregationBuilder extends MultiValuesSourceAggregationBuilder.LeafOnly<TTestAggregationBuilder> {
    public static final String NAME = "t_test";
    public static final ParseField A_FIELD = new ParseField("a");
    public static final ParseField B_FIELD = new ParseField("b");
    public static final ParseField TYPE_FIELD = new ParseField("type");
    public static final ParseField TAILS_FIELD = new ParseField("tails");

    public static final ObjectParser<TTestAggregationBuilder, String> PARSER = ObjectParser.fromBuilder(NAME, TTestAggregationBuilder::new);

    static {
        MultiValuesSourceParseHelper.declareCommon(PARSER, true, ValueType.NUMERIC);
        MultiValuesSourceParseHelper.declareField(A_FIELD.getPreferredName(), PARSER, true, false, true, false, false);
        MultiValuesSourceParseHelper.declareField(B_FIELD.getPreferredName(), PARSER, true, false, true, false, false);
        PARSER.declareString(TTestAggregationBuilder::testType, TYPE_FIELD);
        PARSER.declareInt(TTestAggregationBuilder::tails, TAILS_FIELD);
    }

    private TTestType testType = TTestType.HETEROSCEDASTIC;

    private int tails = 2;

    public static void registerUsage(ValuesSourceRegistry.Builder builder) {
        builder.registerUsage(NAME, CoreValuesSourceType.NUMERIC);
    }

    public TTestAggregationBuilder(String name) {
        super(name);
    }

    public TTestAggregationBuilder(
        TTestAggregationBuilder clone,
        AggregatorFactories.Builder factoriesBuilder,
        Map<String, Object> metadata
    ) {
        super(clone, factoriesBuilder, metadata);
    }

    public TTestAggregationBuilder a(MultiValuesSourceFieldConfig valueConfig) {
        field(A_FIELD.getPreferredName(), Objects.requireNonNull(valueConfig, "Configuration for field [" + A_FIELD + "] cannot be null"));
        return this;
    }

    public TTestAggregationBuilder b(MultiValuesSourceFieldConfig weightConfig) {
        field(B_FIELD.getPreferredName(), Objects.requireNonNull(weightConfig, "Configuration for field [" + B_FIELD + "] cannot be null"));
        return this;
    }

    public TTestAggregationBuilder testType(String testType) {
        return testType(TTestType.resolve(Objects.requireNonNull(testType, "Test type cannot be null")));
    }

    public TTestAggregationBuilder testType(TTestType testType) {
        this.testType = Objects.requireNonNull(testType, "Test type cannot be null");
        return this;
    }

    public TTestAggregationBuilder tails(int tails) {
        if (tails < 1 || tails > 2) {
            throw new IllegalArgumentException("[tails] must be 1 or 2. Found [" + tails + "] in [" + name + "]");
        }
        this.tails = tails;
        return this;
    }

    public TTestAggregationBuilder(StreamInput in) throws IOException {
        super(in);
        testType = in.readEnum(TTestType.class);
        tails = in.readVInt();
    }

    @Override
    protected AggregationBuilder shallowCopy(AggregatorFactories.Builder factoriesBuilder, Map<String, Object> metadata) {
        return new TTestAggregationBuilder(this, factoriesBuilder, metadata);
    }

    @Override
    public boolean supportsSampling() {
        return true;
    }

    @Override
    public BucketCardinality bucketCardinality() {
        return BucketCardinality.NONE;
    }

    @Override
    protected void innerWriteTo(StreamOutput out) throws IOException {
        out.writeEnum(testType);
        out.writeVInt(tails);
    }

    @Override
    protected ValuesSourceType defaultValueSourceType() {
        return CoreValuesSourceType.NUMERIC;
    }

    @Override
    protected MultiValuesSourceAggregatorFactory innerBuild(
        AggregationContext context,
        Map<String, ValuesSourceConfig> configs,
        Map<String, QueryBuilder> filters,
        DocValueFormat format,
        AggregatorFactory parent,
        AggregatorFactories.Builder subFactoriesBuilder
    ) throws IOException {
        QueryBuilder filterA = filters.get(A_FIELD.getPreferredName());
        QueryBuilder filterB = filters.get(B_FIELD.getPreferredName());
        if (filterA == null && filterB == null) {
            FieldContext fieldContextA = configs.get(A_FIELD.getPreferredName()).fieldContext();
            FieldContext fieldContextB = configs.get(B_FIELD.getPreferredName()).fieldContext();
            if (fieldContextA != null && fieldContextB != null) {
                if (fieldContextA.field().equals(fieldContextB.field())) {
                    throw new IllegalArgumentException(
                        "The same field [" + fieldContextA.field() + "] is used for both population but no filters are specified."
                    );
                }
            }
        }

        return new TTestAggregatorFactory(
            name,
            configs,
            testType,
            tails,
            filterA,
            filterB,
            format,
            context,
            parent,
            subFactoriesBuilder,
            metadata
        );
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, ToXContent.Params params) throws IOException {
        return builder;
    }

    @Override
    public String getType() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.ZERO;
    }
}

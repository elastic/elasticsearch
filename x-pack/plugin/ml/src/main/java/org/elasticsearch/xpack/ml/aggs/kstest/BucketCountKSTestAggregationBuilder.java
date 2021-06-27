/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.kstest;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.aggregations.pipeline.BucketHelpers;
import org.elasticsearch.search.aggregations.pipeline.BucketMetricsPipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;

import java.io.IOException;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.elasticsearch.search.aggregations.pipeline.PipelineAggregator.Parser.GAP_POLICY;

public class BucketCountKSTestAggregationBuilder extends BucketMetricsPipelineAggregationBuilder<BucketCountKSTestAggregationBuilder> {

    public static final ParseField NAME = new ParseField("bucket_count_ks_test");
    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<BucketCountKSTestAggregationBuilder, String> PARSER = new ConstructingObjectParser<>(
        NAME.getPreferredName(),
        false,
        (args, context) -> new BucketCountKSTestAggregationBuilder(
            context,
            (String) args[0],
            (List<Double>) args[1],
            (List<String>) args[2],
            (BucketHelpers.GapPolicy) args[3],
            (SamplingMethod) args[4]
        )
    );
    private static final ParseField ALTERNATIVE = new ParseField("alternative");
    private static final ParseField SAMPLING_METHOD = new ParseField("sampling_method");
    private static final ParseField FRACTIONS = new ParseField("fractions");

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), BUCKETS_PATH_FIELD);
        PARSER.declareDoubleArray(ConstructingObjectParser.optionalConstructorArg(), FRACTIONS);
        PARSER.declareStringArray(ConstructingObjectParser.optionalConstructorArg(), ALTERNATIVE);
        PARSER.declareField(ConstructingObjectParser.optionalConstructorArg(), p -> {
            if (p.currentToken() == XContentParser.Token.VALUE_STRING) {
                return BucketHelpers.GapPolicy.parse(p.text().toLowerCase(Locale.ROOT), p.getTokenLocation());
            }
            throw new IllegalArgumentException("Unsupported token [" + p.currentToken() + "]");
        }, GAP_POLICY, ObjectParser.ValueType.STRING);
        PARSER.declareField(ConstructingObjectParser.optionalConstructorArg(), p -> {
            if (p.currentToken() == XContentParser.Token.VALUE_STRING) {
                return SamplingMethod.fromString(p.text().toLowerCase(Locale.ROOT));
            }
            throw new IllegalArgumentException("Unsupported token [" + p.currentToken() + "]");
        }, SAMPLING_METHOD, ObjectParser.ValueType.STRING);
    }

    private final double[] fractions;
    private final EnumSet<Alternative> alternative;
    private final SamplingMethod samplingMethod;
    private BucketCountKSTestAggregationBuilder(
        String name,
        String bucketsPath,
        @Nullable List<Double> fractions,
        @Nullable List<String> alternative,
        @Nullable BucketHelpers.GapPolicy gapPolicy,
        @Nullable SamplingMethod samplingMethod
    ) {
        super(
            name,
            NAME.getPreferredName(),
            new String[] { bucketsPath },
            null,
            gapPolicy == null ? BucketHelpers.GapPolicy.INSERT_ZEROS : gapPolicy
        );
        this.fractions = fractions == null ? null : fractions.stream().mapToDouble(Double::doubleValue).toArray();
        if (alternative == null) {
            this.alternative = EnumSet.allOf(Alternative.class);
        } else {
            if (alternative.isEmpty()) {
                throw new IllegalArgumentException("[alternative] must not be empty for aggregation [" + name + "]");
            }
            List<Alternative> alternativeEnums = alternative.stream().map(Alternative::fromString).collect(Collectors.toList());
            this.alternative = EnumSet.copyOf(alternativeEnums);
        }
        this.samplingMethod = samplingMethod == null ? new SamplingMethod.UpperTail() : samplingMethod;
        if (gapPolicy != null && gapPolicy.equals(BucketHelpers.GapPolicy.INSERT_ZEROS) == false) {
            throw new IllegalArgumentException(
                "only [" + GAP_POLICY.getPreferredName() + "] of [" + BucketHelpers.GapPolicy.INSERT_ZEROS.getName() + "] is supported"
            );
        }
    }

    public BucketCountKSTestAggregationBuilder(
        String name,
        String bucketsPath,
        List<Double> fractions,
        List<String> alternative,
        @Nullable SamplingMethod samplingMethod
    ) {
        this(name, bucketsPath, fractions, alternative, BucketHelpers.GapPolicy.INSERT_ZEROS, samplingMethod);
    }

    public BucketCountKSTestAggregationBuilder(StreamInput in) throws IOException {
        super(in, NAME.getPreferredName());
        this.fractions = in.readBoolean() ? in.readDoubleArray() : null;
        this.alternative = in.readEnumSet(Alternative.class);
        this.samplingMethod = SamplingMethod.fromStream(in);
    }

    public static SearchPlugin.PipelineAggregationSpec buildSpec() {
        return new SearchPlugin.PipelineAggregationSpec(
            NAME,
            BucketCountKSTestAggregationBuilder::new,
            BucketCountKSTestAggregationBuilder.PARSER
        );
    }

    @Override
    public String getWriteableName() {
        return NAME.getPreferredName();
    }

    @Override
    protected void innerWriteTo(StreamOutput out) throws IOException {
        out.writeBoolean(fractions != null);
        if (fractions != null) {
            out.writeDoubleArray(fractions);
        }
        out.writeEnumSet(alternative);
        out.writeString(samplingMethod.getName());
    }

    @Override
    protected PipelineAggregator createInternal(Map<String, Object> metadata) {
        return new BucketCountKSTestAggregator(name, fractions, alternative, bucketsPaths[0], samplingMethod, metadata);
    }

    @Override
    protected boolean overrideBucketsPath() {
        return true;
    }

    @Override
    protected XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.field(BUCKETS_PATH_FIELD.getPreferredName(), bucketsPaths[0]);
        if (fractions != null) {
            builder.field(FRACTIONS.getPreferredName(), fractions);
        }
        builder.field(ALTERNATIVE.getPreferredName(), alternative);
        builder.field(SAMPLING_METHOD.getPreferredName(), samplingMethod.getName());
        return builder;
    }

    @Override
    protected void validate(ValidationContext context) {
        super.validate(context);
        if (bucketsPaths[0].endsWith("_count") == false) {
            context.addBucketPathValidationError("[bucket_count_ks_test] requires that buckets_path points to bucket [_count]");
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (super.equals(o) == false) return false;
        BucketCountKSTestAggregationBuilder that = (BucketCountKSTestAggregationBuilder) o;
        return Arrays.equals(fractions, that.fractions)
            && Objects.equals(alternative, that.alternative)
            && Objects.equals(samplingMethod, that.samplingMethod);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), Arrays.hashCode(fractions), alternative, samplingMethod);
    }
}

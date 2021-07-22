/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.correlation;

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
import org.elasticsearch.xpack.core.ml.utils.NamedXContentObjectHelper;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.search.aggregations.pipeline.PipelineAggregator.Parser.GAP_POLICY;

public class BucketCorrelationAggregationBuilder extends BucketMetricsPipelineAggregationBuilder<BucketCorrelationAggregationBuilder> {

    public static final ParseField NAME = new ParseField("bucket_correlation");
    private static final ParseField FUNCTION = new ParseField("function");

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<BucketCorrelationAggregationBuilder, String> PARSER = new ConstructingObjectParser<>(
        NAME.getPreferredName(),
        false,
        (args, context) -> new BucketCorrelationAggregationBuilder(
            context,
            (String)args[0],
            (CorrelationFunction)args[1],
            (BucketHelpers.GapPolicy)args[2]
        )
    );
    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), BUCKETS_PATH_FIELD);
        PARSER.declareNamedObject(
            ConstructingObjectParser.constructorArg(),
            (p, c, n) -> p.namedObject(CorrelationFunction.class, n, null),
            FUNCTION
        );
        PARSER.declareField(ConstructingObjectParser.optionalConstructorArg(), p -> {
            if (p.currentToken() == XContentParser.Token.VALUE_STRING) {
                return BucketHelpers.GapPolicy.parse(p.text().toLowerCase(Locale.ROOT), p.getTokenLocation());
            }
            throw new IllegalArgumentException("Unsupported token [" + p.currentToken() + "]");
        }, GAP_POLICY, ObjectParser.ValueType.STRING);
    }

    public static SearchPlugin.PipelineAggregationSpec buildSpec() {
        return new SearchPlugin.PipelineAggregationSpec(
            NAME,
            BucketCorrelationAggregationBuilder::new,
            BucketCorrelationAggregationBuilder.PARSER
        );
    }

    private final CorrelationFunction correlationFunction;

    public BucketCorrelationAggregationBuilder(String name, String bucketsPath, CorrelationFunction correlationFunction) {
        this(name, bucketsPath, correlationFunction, BucketHelpers.GapPolicy.INSERT_ZEROS);
    }

    private BucketCorrelationAggregationBuilder(
        String name,
        String bucketsPath,
        CorrelationFunction correlationFunction,
        BucketHelpers.GapPolicy gapPolicy
    ) {
        super(
            name,
            NAME.getPreferredName(),
            new String[] {bucketsPath},
            null,
            gapPolicy == null ? BucketHelpers.GapPolicy.INSERT_ZEROS : gapPolicy
        );
        this.correlationFunction = correlationFunction;
        if (gapPolicy != null && gapPolicy.equals(BucketHelpers.GapPolicy.INSERT_ZEROS) == false) {
            throw new IllegalArgumentException(
                "only [gap_policy] of [" + BucketHelpers.GapPolicy.INSERT_ZEROS.getName() + "] is supported"
            );
        }
    }

    public BucketCorrelationAggregationBuilder(StreamInput in) throws IOException {
        super(in, NAME.getPreferredName());
        this.correlationFunction = in.readNamedWriteable(CorrelationFunction.class);
    }

    @Override
    public String getWriteableName() {
        return NAME.getPreferredName();
    }

    @Override
    protected void innerWriteTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(correlationFunction);
    }

    @Override
    protected PipelineAggregator createInternal(Map<String, Object> metadata) {
        return new BucketCorrelationAggregator(name, correlationFunction, bucketsPaths[0], metadata);
    }

    @Override
    protected boolean overrideBucketsPath() {
        return true;
    }

    @Override
    protected XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.field(BUCKETS_PATH_FIELD.getPreferredName(), bucketsPaths[0]);
        NamedXContentObjectHelper.writeNamedObject(builder, params, FUNCTION.getPreferredName(), correlationFunction);
        return builder;
    }

    @Override
    protected void validate(ValidationContext context) {
        super.validate(context);
        correlationFunction.validate(context, bucketsPaths[0]);
    }

    @Override
    public boolean equals(Object o) {
        if (super.equals(o) == false) return false;
        BucketCorrelationAggregationBuilder that = (BucketCorrelationAggregationBuilder) o;
        return Objects.equals(correlationFunction, that.correlationFunction);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), correlationFunction);
    }
}

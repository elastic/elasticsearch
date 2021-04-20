/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.correlation;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.AbstractPipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.xpack.core.ml.utils.NamedXContentObjectHelper;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public class BucketCorrelationAggregationBuilder extends AbstractPipelineAggregationBuilder<BucketCorrelationAggregationBuilder> {

    public static final ParseField NAME = new ParseField("bucket_correlation");
    private static final ParseField FUNCTION = new ParseField("function");

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<BucketCorrelationAggregationBuilder, String> PARSER = new ConstructingObjectParser<>(
        NAME.getPreferredName(),
        false,
        (args, context) -> new BucketCorrelationAggregationBuilder(
            context,
            (String)args[0],
            (CorrelationFunction)args[1]
        )
    );
    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), BUCKETS_PATH_FIELD);
        PARSER.declareNamedObject(
            ConstructingObjectParser.constructorArg(),
            (p, c, n) -> p.namedObject(CorrelationFunction.class, n, null),
            FUNCTION
        );
    }

    private final CorrelationFunction correlationFunction;

    public BucketCorrelationAggregationBuilder(
        String name,
        String bucketsPath,
        CorrelationFunction correlationFunction
    ) {
        super(
            name,
            NAME.getPreferredName(),
            new String[] {bucketsPath}
        );
        this.correlationFunction = correlationFunction;
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
    protected void doWriteTo(StreamOutput out) throws IOException {
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
    protected XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(BUCKETS_PATH_FIELD.getPreferredName(), bucketsPaths[0]);
        NamedXContentObjectHelper.writeNamedObject(builder, params, FUNCTION.getPreferredName(), correlationFunction);
        return builder;
    }

    @Override
    protected void validate(ValidationContext context) {

        final String firstAgg = bucketsPaths[0].split("[>\\.]")[0];
        Optional<AggregationBuilder> aggBuilder = context.getSiblingAggregations().stream()
            .filter(builder -> builder.getName().equals(firstAgg))
            .findAny();
        if (aggBuilder.isEmpty()) {
            context.addBucketPathValidationError("aggregation does not exist for aggregation [" + name + "]: " + bucketsPaths[0]);
            return;
        }
        AggregationBuilder aggregationBuilder = aggBuilder.get();
        if (aggregationBuilder.bucketCardinality() != AggregationBuilder.BucketCardinality.MANY) {
            context.addValidationError("The first aggregation in " + PipelineAggregator.Parser.BUCKETS_PATH.getPreferredName()
                + " must be a multi-bucket aggregation for aggregation [" + name + "] found :"
                + aggBuilder.get().getClass().getName() + " for buckets path: " + bucketsPaths[0]);
            return;
        }
        correlationFunction.validate(context, bucketsPaths[0]);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (super.equals(o) == false) return false;
        BucketCorrelationAggregationBuilder that = (BucketCorrelationAggregationBuilder) o;
        return Objects.equals(correlationFunction, that.correlationFunction);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), correlationFunction);
    }
}

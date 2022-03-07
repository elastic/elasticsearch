/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.changepoint;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.aggregations.pipeline.BucketHelpers;
import org.elasticsearch.search.aggregations.pipeline.BucketMetricsPipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.search.aggregations.pipeline.PipelineAggregator.Parser.GAP_POLICY;

public class ChangePointAggregationBuilder extends BucketMetricsPipelineAggregationBuilder<ChangePointAggregationBuilder> {

    public static final ParseField NAME = new ParseField("change_point");
    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<ChangePointAggregationBuilder, String> PARSER = new ConstructingObjectParser<>(
        NAME.getPreferredName(),
        false,
        (args, context) -> new ChangePointAggregationBuilder(context, (String) args[0])
    );

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), BUCKETS_PATH_FIELD);
        PARSER.declareField(
            ConstructingObjectParser.optionalConstructorArg(),
            p -> BucketHelpers.GapPolicy.parse(p.text().toLowerCase(Locale.ROOT), p.getTokenLocation()),
            GAP_POLICY,
            ObjectParser.ValueType.STRING
        );
    }

    public ChangePointAggregationBuilder(String name, String bucketsPath) {
        super(name, NAME.getPreferredName(), new String[] { bucketsPath });
    }

    public ChangePointAggregationBuilder(StreamInput in) throws IOException {
        super(in, NAME.getPreferredName());
    }

    public static SearchPlugin.PipelineAggregationSpec buildSpec() {
        return new SearchPlugin.PipelineAggregationSpec(NAME, ChangePointAggregationBuilder::new, ChangePointAggregationBuilder.PARSER)
            .addResultReader(InternalChangePointAggregation::new);
    }

    @Override
    public String getWriteableName() {
        return NAME.getPreferredName();
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return Version.V_8_2_0;
    }

    @Override
    protected void innerWriteTo(StreamOutput out) throws IOException {}

    @Override
    protected PipelineAggregator createInternal(Map<String, Object> metadata) {
        return new ChangePointAggregator(name, bucketsPaths[0], metadata);
    }

    @Override
    protected boolean overrideBucketsPath() {
        return true;
    }

    @Override
    protected XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.field(BUCKETS_PATH_FIELD.getPreferredName(), bucketsPaths[0]);
        return builder;
    }

}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.dataframe.transforms.pivot;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregationBuilder;
import org.elasticsearch.xpack.core.dataframe.DataFrameField;
import org.elasticsearch.xpack.core.dataframe.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class PivotConfig implements Writeable, ToXContentObject {

    private static final String NAME = "data_frame_transform_pivot";
    private final GroupConfig groups;
    private final AggregationConfig aggregationConfig;

    private static final ConstructingObjectParser<PivotConfig, Void> STRICT_PARSER = createParser(false);
    private static final ConstructingObjectParser<PivotConfig, Void> LENIENT_PARSER = createParser(true);

    private static ConstructingObjectParser<PivotConfig, Void> createParser(boolean lenient) {
        ConstructingObjectParser<PivotConfig, Void> parser = new ConstructingObjectParser<>(NAME, lenient,
                args -> {
                    GroupConfig groups = (GroupConfig) args[0];

                    // allow "aggs" and "aggregations" but require one to be specified
                    // if somebody specifies both: throw
                    AggregationConfig aggregationConfig = null;
                    if (args[1] != null) {
                        aggregationConfig = (AggregationConfig) args[1];
                    }

                    if (args[2] != null) {
                        if (aggregationConfig != null) {
                            throw new IllegalArgumentException("Found two aggregation definitions: [aggs] and [aggregations]");
                        }
                        aggregationConfig = (AggregationConfig) args[2];
                    }
                    if (aggregationConfig == null) {
                        throw new IllegalArgumentException("Required [aggregations]");
                    }

                    return new PivotConfig(groups, aggregationConfig);
                });

        parser.declareObject(constructorArg(),
                (p, c) -> (GroupConfig.fromXContent(p, lenient)), DataFrameField.GROUP_BY);

        parser.declareObject(optionalConstructorArg(), (p, c) -> AggregationConfig.fromXContent(p, lenient), DataFrameField.AGGREGATIONS);
        parser.declareObject(optionalConstructorArg(), (p, c) -> AggregationConfig.fromXContent(p, lenient), DataFrameField.AGGS);

        return parser;
    }

    public PivotConfig(final GroupConfig groups, final AggregationConfig aggregationConfig) {
        this.groups = ExceptionsHelper.requireNonNull(groups, DataFrameField.GROUP_BY.getPreferredName());
        this.aggregationConfig = ExceptionsHelper.requireNonNull(aggregationConfig, DataFrameField.AGGREGATIONS.getPreferredName());
    }

    public PivotConfig(StreamInput in) throws IOException {
        this.groups = new GroupConfig(in);
        this.aggregationConfig = new AggregationConfig(in);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(DataFrameField.GROUP_BY.getPreferredName(), groups);
        builder.field(DataFrameField.AGGREGATIONS.getPreferredName(), aggregationConfig);
        builder.endObject();
        return builder;
    }

    public void toCompositeAggXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(CompositeAggregationBuilder.SOURCES_FIELD_NAME.getPreferredName());
        builder.startArray();

        for (Entry<String, SingleGroupSource> groupBy : groups.getGroups().entrySet()) {
            builder.startObject();
            builder.startObject(groupBy.getKey());
            builder.field(groupBy.getValue().getType().value(), groupBy.getValue());
            builder.endObject();
            builder.endObject();
        }

        builder.endArray();
        builder.endObject(); // sources
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        groups.writeTo(out);
        aggregationConfig.writeTo(out);
    }

    public AggregationConfig getAggregationConfig() {
        return aggregationConfig;
    }

    public GroupConfig getGroupConfig() {
        return groups;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        final PivotConfig that = (PivotConfig) other;

        return Objects.equals(this.groups, that.groups) && Objects.equals(this.aggregationConfig, that.aggregationConfig);
    }

    @Override
    public int hashCode() {
        return Objects.hash(groups, aggregationConfig);
    }

    public boolean isValid() {
        return groups.isValid() && aggregationConfig.isValid();
    }

    public static PivotConfig fromXContent(final XContentParser parser, boolean lenient) throws IOException {
        return lenient ? LENIENT_PARSER.apply(parser, null) : STRICT_PARSER.apply(parser, null);
    }
}

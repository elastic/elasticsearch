/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.dataframe.transforms.pivot;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregationBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;

public class PivotConfig implements Writeable, ToXContentObject {

    private static final String NAME = "data_frame_transform_pivot";
    private static final ParseField GROUP_BY = new ParseField("group_by");
    private static final ParseField AGGREGATIONS = new ParseField("aggregations");

    private final List<GroupConfig> groups;
    private final AggregationConfig aggregationConfig;

    private static final ConstructingObjectParser<PivotConfig, Void> PARSER = createParser(false);
    private static final ConstructingObjectParser<PivotConfig, Void> LENIENT_PARSER = createParser(true);

    private static ConstructingObjectParser<PivotConfig, Void> createParser(boolean ignoreUnknownFields) {
        ConstructingObjectParser<PivotConfig, Void> parser = new ConstructingObjectParser<>(NAME, ignoreUnknownFields,
                args -> {
                    @SuppressWarnings("unchecked")
                    List<GroupConfig> groups = (List<GroupConfig>) args[0];
                    AggregationConfig aggregationConfig = (AggregationConfig) args[1];
                    return new PivotConfig(groups, aggregationConfig);
                });

        parser.declareObjectArray(constructorArg(),
                (p, c) -> (GroupConfig.fromXContent(p, ignoreUnknownFields)), GROUP_BY);

        parser.declareObject(constructorArg(), (p, c) -> AggregationConfig.fromXContent(p), AGGREGATIONS);

        return parser;
    }

    public PivotConfig(final List<GroupConfig> groups, final AggregationConfig aggregationConfig) {
        this.groups = groups;
        this.aggregationConfig = aggregationConfig;
    }

    public PivotConfig(StreamInput in) throws IOException {
        this.groups = in.readList(GroupConfig::new);
        this.aggregationConfig = new AggregationConfig(in);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(GROUP_BY.getPreferredName(), groups);
        builder.field(AGGREGATIONS.getPreferredName(), aggregationConfig);
        builder.endObject();
        return builder;
    }

    public void toCompositeAggXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(CompositeAggregationBuilder.SOURCES_FIELD_NAME.getPreferredName());
        builder.startArray();
        for (GroupConfig group:groups) {
            group.toXContent(builder, params);
        }
        builder.endArray();
        builder.endObject(); // sources
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeList(groups);
        aggregationConfig.writeTo(out);
    }

    public AggregationConfig getAggregationConfig() {
        return aggregationConfig;
    }

    public Iterable<GroupConfig> getGroups() {
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

    public static PivotConfig fromXContent(final XContentParser parser, boolean ignoreUnknownFields) throws IOException {
        if (ignoreUnknownFields) {
            return LENIENT_PARSER.apply(parser, null);
        }
        // else
        return PARSER.apply(parser, null);
    }
}

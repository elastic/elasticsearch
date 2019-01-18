/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.dataframe.transforms;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeValuesSourceBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeValuesSourceParserHelper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;

/*
 * Wrapper for the Source config part of a composite aggregation.
 *
 * For now just wraps sources from composite aggs.
 */
public class SourceConfig implements Writeable, ToXContentObject {

    private static final String NAME = "data_frame_source";

    private final List<CompositeValuesSourceBuilder<?>> sources;

    private static final ConstructingObjectParser<SourceConfig, String> PARSER = new ConstructingObjectParser<>(NAME, false, (args) -> {
        @SuppressWarnings("unchecked")
        List<CompositeValuesSourceBuilder<?>> sources = (List<CompositeValuesSourceBuilder<?>>) args[0];
        return new SourceConfig(sources);
    });

    static {
        PARSER.declareFieldArray(constructorArg(), (parser, builder) -> CompositeValuesSourceParserHelper.fromXContent(parser),
                CompositeAggregationBuilder.SOURCES_FIELD_NAME, ObjectParser.ValueType.OBJECT_ARRAY);
    }

    SourceConfig(final StreamInput in) throws IOException {
        int num = in.readVInt();
        List<CompositeValuesSourceBuilder<?>> sources = new ArrayList<>(num);
        for (int i = 0; i < num; i++) {
            CompositeValuesSourceBuilder<?> builder = CompositeValuesSourceParserHelper.readFrom(in);
            sources.add(builder);
        }
        this.sources = Collections.unmodifiableList(sources);
    }

    public SourceConfig(List<CompositeValuesSourceBuilder<?>> sources) {
        this.sources = Collections.unmodifiableList(sources);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.startArray(CompositeAggregationBuilder.SOURCES_FIELD_NAME.getPreferredName());
        for (CompositeValuesSourceBuilder<?> source : getSources()) {
            CompositeValuesSourceParserHelper.toXContent(source, builder, params);
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(getSources().size());
        for (CompositeValuesSourceBuilder<?> builder : getSources()) {
            CompositeValuesSourceParserHelper.writeTo(builder, out);
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(getSources());
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        final SourceConfig that = (SourceConfig) other;

        return Objects.equals(this.getSources(), that.getSources());
    }

    public static SourceConfig fromXContent(final XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    public List<CompositeValuesSourceBuilder<?>> getSources() {
        return sources;
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.dataframe.transform;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.dataframe.DataFrameField;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * This class holds the configuration details of a data frame transform
 */
public class DataFrameTransformConfig implements NamedWriteable, ToXContentObject {

    private static final String NAME = "xpack/data_frame/transformconfig";
    private static final ParseField INDEX_PATTERN = new ParseField("index_pattern");
    private static final ParseField DESTINATION_INDEX = new ParseField("destination_index");
    private static final ParseField SOURCES = new ParseField("sources");
    private static final ParseField AGGREGATIONS = new ParseField("aggregations");

    private final String id;
    private final String indexPattern;
    private final String destinationIndex;
    private final SourceConfig sourceConfig;
    private final AggregationConfig aggregationConfig;

    public static final ConstructingObjectParser<DataFrameTransformConfig, String> PARSER = createParser(false);
    public static final ConstructingObjectParser<DataFrameTransformConfig, String> LENIENT_PARSER = createParser(true);

    private static ConstructingObjectParser<DataFrameTransformConfig, String> createParser(boolean ignoreUnknownFields) {
        ConstructingObjectParser<DataFrameTransformConfig, String> parser = new ConstructingObjectParser<>(NAME, ignoreUnknownFields,
                (args, optionalId) -> {
                    String id = args[0] != null ? (String) args[0] : optionalId;
                    String indexPattern = (String) args[1];
                    String destinationIndex = (String) args[2];
                    SourceConfig sourceConfig = (SourceConfig) args[3];
                    AggregationConfig aggregationConfig = (AggregationConfig) args[4];
                    return new DataFrameTransformConfig(id, indexPattern, destinationIndex, sourceConfig, aggregationConfig);
                });

        parser.declareString(optionalConstructorArg(), DataFrameField.ID);
        parser.declareString(constructorArg(), INDEX_PATTERN);
        parser.declareString(constructorArg(), DESTINATION_INDEX);
        parser.declareObject(optionalConstructorArg(), (p, c) -> SourceConfig.fromXContent(p), SOURCES);
        parser.declareObject(optionalConstructorArg(), (p, c) -> AggregationConfig.fromXContent(p), AGGREGATIONS);

        return parser;
    }

    public static String documentId(String transformId) {
        return "dataframe-" + transformId;
    }

    public DataFrameTransformConfig(final String id,
                                        final String indexPattern,
                                        final String destinationIndex,
                                        final SourceConfig sourceConfig,
                                        final AggregationConfig aggregationConfig) {
        this.id = ExceptionsHelper.requireNonNull(id, DataFrameField.ID.getPreferredName());
        this.indexPattern = ExceptionsHelper.requireNonNull(indexPattern, INDEX_PATTERN.getPreferredName());
        this.destinationIndex = ExceptionsHelper.requireNonNull(destinationIndex, DESTINATION_INDEX.getPreferredName());

        // TODO: check for null?
        this.sourceConfig = sourceConfig;
        this.aggregationConfig = aggregationConfig;
    }

    public DataFrameTransformConfig(final StreamInput in) throws IOException {
        id = in.readString();
        indexPattern = in.readString();
        destinationIndex = in.readString();
        sourceConfig = in.readOptionalWriteable(SourceConfig::new);
        aggregationConfig = in.readOptionalWriteable(AggregationConfig::new);
    }

    public String getId() {
        return id;
    }

    public String getCron() {
        return "*";
    }

    public String getIndexPattern() {
        return indexPattern;
    }

    public String getDestinationIndex() {
        return destinationIndex;
    }

    public SourceConfig getSourceConfig() {
        return sourceConfig;
    }

    public AggregationConfig getAggregationConfig() {
        return aggregationConfig;
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        out.writeString(id);
        out.writeString(indexPattern);
        out.writeString(destinationIndex);
        out.writeOptionalWriteable(sourceConfig);
        out.writeOptionalWriteable(aggregationConfig);
    }

    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
        builder.startObject();
        builder.field(DataFrameField.ID.getPreferredName(), id);
        builder.field(INDEX_PATTERN.getPreferredName(), indexPattern);
        builder.field(DESTINATION_INDEX.getPreferredName(), destinationIndex);
        if (sourceConfig != null) {
            builder.field(SOURCES.getPreferredName(), sourceConfig);
        }
        if (aggregationConfig!=null) {
            builder.field(AGGREGATIONS.getPreferredName(), aggregationConfig);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        final DataFrameTransformConfig that = (DataFrameTransformConfig) other;

        return Objects.equals(this.id, that.id)
                && Objects.equals(this.indexPattern, that.indexPattern)
                && Objects.equals(this.destinationIndex, that.destinationIndex)
                && Objects.equals(this.sourceConfig, that.sourceConfig)
                && Objects.equals(this.aggregationConfig, that.aggregationConfig);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, indexPattern, destinationIndex, sourceConfig, aggregationConfig);
    }

    @Override
    public String toString() {
        return Strings.toString(this, true, true);
    }

    public static DataFrameTransformConfig fromXContent(final XContentParser parser, @Nullable final String optionalTransformId)
            throws IOException {
        return PARSER.parse(parser, optionalTransformId);
    }
}

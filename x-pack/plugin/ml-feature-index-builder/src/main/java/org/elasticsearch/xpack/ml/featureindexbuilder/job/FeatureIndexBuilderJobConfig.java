/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.featureindexbuilder.job;

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
import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * This class holds the configuration details of a feature index builder job
 */
public class FeatureIndexBuilderJobConfig implements NamedWriteable, ToXContentObject {

    private static final String NAME = "xpack/feature_index_builder/jobconfig";
    private static final ParseField ID = new ParseField("id");

    private final String id;

    private static final ConstructingObjectParser<FeatureIndexBuilderJobConfig, String> PARSER = new ConstructingObjectParser<>(NAME, false,
            (args, optionalId) -> {
                String id = args[0] != null ? (String) args[0] : optionalId;
                return new FeatureIndexBuilderJobConfig(id);
            });

    static {
        PARSER.declareString(optionalConstructorArg(), ID);
    }

    public FeatureIndexBuilderJobConfig(final String id) {
        this.id = id;
    }

    public FeatureIndexBuilderJobConfig(final StreamInput in) throws IOException {
        id = in.readString();
    }

    public String getId() {
        return id;
    }

    public String getCron() {
        return "*";
    }

    public void writeTo(final StreamOutput out) throws IOException {
        out.writeString(id);
    }

    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
        builder.startObject();
        builder.field(ID.getPreferredName(), id);
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

        final FeatureIndexBuilderJobConfig that = (FeatureIndexBuilderJobConfig) other;

        return Objects.equals(this.id, that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    @Override
    public String toString() {
        return Strings.toString(this, true, true);
    }

    public static FeatureIndexBuilderJobConfig fromXContent(final XContentParser parser, @Nullable final String optionalJobId)
            throws IOException {
        return PARSER.parse(parser, optionalJobId);
    }
}

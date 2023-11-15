/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

public record TrainedModelPrefixStrings(String ingestPrefix, String searchPrefix) implements ToXContentObject, Writeable {

    public enum PrefixType {
        INGEST,
        SEARCH,
        NONE
    }

    public static final ParseField INGEST_PREFIX = new ParseField("ingest");
    public static final ParseField SEARCH_PREFIX = new ParseField("search");
    public static final String NAME = "trained_model_config_prefix_strings";

    private static final ConstructingObjectParser<TrainedModelPrefixStrings, Void> LENIENT_PARSER = createParser(true);
    private static final ConstructingObjectParser<TrainedModelPrefixStrings, Void> STRICT_PARSER = createParser(false);

    @SuppressWarnings("unchecked")
    private static ConstructingObjectParser<TrainedModelPrefixStrings, Void> createParser(boolean ignoreUnknownFields) {
        ConstructingObjectParser<TrainedModelPrefixStrings, Void> parser = new ConstructingObjectParser<>(
            NAME,
            ignoreUnknownFields,
            a -> new TrainedModelPrefixStrings((String) a[0], (String) a[1])
        );
        parser.declareString(ConstructingObjectParser.optionalConstructorArg(), INGEST_PREFIX);
        parser.declareString(ConstructingObjectParser.optionalConstructorArg(), SEARCH_PREFIX);
        return parser;
    }

    public static TrainedModelPrefixStrings fromXContent(XContentParser parser, boolean lenient) throws IOException {
        return lenient ? LENIENT_PARSER.parse(parser, null) : STRICT_PARSER.parse(parser, null);
    }

    public TrainedModelPrefixStrings(StreamInput in) throws IOException {
        this(in.readOptionalString(), in.readOptionalString());
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (ingestPrefix != null) {
            builder.field(INGEST_PREFIX.getPreferredName(), ingestPrefix);
        }
        if (searchPrefix != null) {
            builder.field(SEARCH_PREFIX.getPreferredName(), searchPrefix);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalString(ingestPrefix);
        out.writeOptionalString(searchPrefix);
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.regionpolicy;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

public record CspRegion(String csp, String region) implements ToXContentObject, Writeable {

    static final ParseField CSP_FIELD = new ParseField("csp");
    static final ParseField REGION_FIELD = new ParseField("region");

    public static final ConstructingObjectParser<CspRegion, Void> STRICT_PARSER = createParser(false);
    public static final ConstructingObjectParser<CspRegion, Void> LENIENT_PARSER = createParser(true);

    static ConstructingObjectParser<CspRegion, Void> createParser(boolean ignoreUnknownFields) {
        ConstructingObjectParser<CspRegion, Void> parser = new ConstructingObjectParser<>(
            "csp_region",
            ignoreUnknownFields,
            args -> new CspRegion((String) args[0], (String) args[1])
        );
        parser.declareString(ConstructingObjectParser.constructorArg(), CSP_FIELD);
        parser.declareString(ConstructingObjectParser.constructorArg(), REGION_FIELD);
        return parser;
    }

    public CspRegion(StreamInput in) throws IOException {
        this(in.readString(), in.readString());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(csp);
        out.writeString(region);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(CSP_FIELD.getPreferredName(), csp);
        builder.field(REGION_FIELD.getPreferredName(), region);
        builder.endObject();
        return builder;
    }
}

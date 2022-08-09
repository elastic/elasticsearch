/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.pytorch.results;

import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

public record AckResult(boolean acknowledged) implements ToXContentObject {

    public static final ParseField ACKNOWLEDGED = new ParseField("acknowledged");

    public static ConstructingObjectParser<AckResult, Void> PARSER = new ConstructingObjectParser<>(
        "ack",
        a -> new AckResult((Boolean) a[0])
    );

    static {
        PARSER.declareBoolean(ConstructingObjectParser.constructorArg(), ACKNOWLEDGED);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(ACKNOWLEDGED.getPreferredName(), acknowledged);
        builder.endObject();
        return builder;
    }
}

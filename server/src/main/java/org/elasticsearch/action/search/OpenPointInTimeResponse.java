/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.search;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

public final class OpenPointInTimeResponse extends ActionResponse implements ToXContentObject {
    private static final ParseField ID = new ParseField("id");
    private static final ParseField MACAROON = new ParseField("macaroon");

    private static final ConstructingObjectParser<OpenPointInTimeResponse, Void> PARSER;

    static {
        PARSER = new ConstructingObjectParser<>("open_point_in_time", true, a -> new OpenPointInTimeResponse((String) a[0], (String) a[1]));
        PARSER.declareField(constructorArg(), (parser, context) -> parser.text(), ID, ObjectParser.ValueType.STRING);
        PARSER.declareStringOrNull(constructorArg(), MACAROON);
    }
    private final String pointInTimeId;
    private final String macaroon;

    public OpenPointInTimeResponse(String pointInTimeId, String macaroon) {
        this.pointInTimeId = Objects.requireNonNull(pointInTimeId, "Point in time parameter must be not null");
        this.macaroon = macaroon;
    }

    public OpenPointInTimeResponse(StreamInput in) throws IOException {
        super(in);
        pointInTimeId = in.readString();
        macaroon = in.readOptionalString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(pointInTimeId);
        out.writeOptionalString(macaroon);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(ID.getPreferredName(), pointInTimeId);
        builder.field(MACAROON.getPreferredName(), macaroon);
        builder.endObject();
        return builder;
    }

    public String getPointInTimeId() {
        return pointInTimeId;
    }

    public String getMacaroon() {
        return macaroon;
    }

    public static OpenPointInTimeResponse fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }
}

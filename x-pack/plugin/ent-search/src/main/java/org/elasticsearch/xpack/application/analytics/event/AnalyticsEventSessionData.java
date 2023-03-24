
/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.analytics.event;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

public class AnalyticsEventSessionData implements ToXContent, Writeable {
    public static ParseField SESSION_FIELD = new ParseField("session");

    public static ParseField SESSION_ID_FIELD = new ParseField("id");

    private static final ConstructingObjectParser<AnalyticsEventSessionData, AnalyticsEvent.Context> PARSER =
        new ConstructingObjectParser<>(SESSION_FIELD.getPreferredName(), false, (p, c) -> new AnalyticsEventSessionData((String) p[0]));

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), SESSION_ID_FIELD);
    }

    private final String id;

    public AnalyticsEventSessionData(String id) {
        this.id = Strings.requireNonBlank(id, "session id can't be empty");
    }

    public AnalyticsEventSessionData(StreamInput in) throws IOException {
        this(in.readString());
    }

    public static AnalyticsEventSessionData fromXContent(XContentParser parser, AnalyticsEvent.Context context) throws IOException {
        return PARSER.parse(parser, context);
    }

    public String id() {
        return id;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        {
            builder.field(SESSION_ID_FIELD.getPreferredName(), id());
        }
        return builder.endObject();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(id);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AnalyticsEventSessionData that = (AnalyticsEventSessionData) o;
        return id.equals(that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }
}

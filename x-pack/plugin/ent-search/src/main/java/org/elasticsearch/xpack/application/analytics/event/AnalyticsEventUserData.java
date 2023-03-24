
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

public class AnalyticsEventUserData implements ToXContent, Writeable {
    public static ParseField USER_FIELD = new ParseField("user");

    public static ParseField USER_ID_FIELD = new ParseField("id");

    private static final ConstructingObjectParser<AnalyticsEventUserData, AnalyticsEvent.Context> PARSER = new ConstructingObjectParser<>(
        USER_FIELD.getPreferredName(),
        false,
        (p, c) -> new AnalyticsEventUserData((String) p[0])
    );

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), USER_ID_FIELD);
    }

    private final String id;

    public AnalyticsEventUserData(String id) {
        this.id = Strings.requireNonBlank(id, "user id can't be empty");
    }

    public AnalyticsEventUserData(StreamInput in) throws IOException {
        this(in.readString());
    }

    public static AnalyticsEventUserData fromXContent(XContentParser parser, AnalyticsEvent.Context context) throws IOException {
        return PARSER.parse(parser, context);
    }

    public String id() {
        return id;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        {
            builder.field(USER_ID_FIELD.getPreferredName(), id());
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
        AnalyticsEventUserData that = (AnalyticsEventUserData) o;
        return id.equals(that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }
}

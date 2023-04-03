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
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

public class AnalyticsEventSortOrderData implements ToXContent, Writeable {

    public static ParseField SORT_FIELD = new ParseField("sort");

    public static ParseField NAME_FIELD = new ParseField("name");

    public static ParseField DIRECTION_FIELD = new ParseField("direction");

    private static final ConstructingObjectParser<AnalyticsEventSortOrderData, AnalyticsEvent.Context> PARSER =
        new ConstructingObjectParser<>(
            SORT_FIELD.getPreferredName(),
            false,
            (p, c) -> new AnalyticsEventSortOrderData((String) p[0], (String) p[1])
        );

    static {
        PARSER.declareString(
            ConstructingObjectParser.constructorArg(),
            s -> Strings.requireNonBlank(s, "field [name] can't be blank"),
            NAME_FIELD
        );

        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), DIRECTION_FIELD);
    }

    private final String name;

    private final String direction;

    public AnalyticsEventSortOrderData(String name, @Nullable String direction) {
        this.name = Objects.requireNonNull(name);
        this.direction = direction;
    }

    protected AnalyticsEventSortOrderData(String name) {
        this(name, null);
    }

    public AnalyticsEventSortOrderData(StreamInput in) throws IOException {
        this(in.readString(), in.readOptionalString());
    }

    public static AnalyticsEventSortOrderData fromXContent(XContentParser parser, AnalyticsEvent.Context context) throws IOException {
        return PARSER.parse(parser, context);
    }

    public String name() {
        return name;
    }

    public String direction() {
        return direction;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject();
        {
            builder.field(NAME_FIELD.getPreferredName(), name());

            if (Objects.nonNull(direction)) {
                builder.field(DIRECTION_FIELD.getPreferredName(), direction());
            }
        }
        return builder.endObject();
    }

    @Override
    public boolean isFragment() {
        return false;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeOptionalString(direction);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AnalyticsEventSortOrderData that = (AnalyticsEventSortOrderData) o;
        return name.equals(that.name) && Objects.equals(direction, that.direction);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, direction);
    }
}

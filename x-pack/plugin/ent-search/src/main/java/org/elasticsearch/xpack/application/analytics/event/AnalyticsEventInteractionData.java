
/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.analytics.event;

import org.apache.logging.log4j.util.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Locale;
import java.util.Objects;

public class AnalyticsEventInteractionData implements ToXContent, Writeable {
    /**
     * Interaction types.
     */
    public enum Type {
        CLICK("click");

        private final String typeName;

        Type(String typeName) {
            this.typeName = typeName;
        }

        public String toString() {
            return typeName.toLowerCase(Locale.ROOT);
        }
    }

    public static ParseField INTERACTION_FIELD = new ParseField("interaction");

    public static ParseField INTERACTION_TYPE_FIELD = new ParseField("type");

    private static final ConstructingObjectParser<AnalyticsEventInteractionData, AnalyticsContext> PARSER = new ConstructingObjectParser<>(
        INTERACTION_FIELD.getPreferredName(),
        false,
        (p, c) -> new AnalyticsEventInteractionData((Type) p[0])
    );

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), (s) -> {
            if (Strings.toRootLowerCase(s).equals(s)) {
                // API expect a lower case value.
                return Type.valueOf(s.toUpperCase(Locale.ROOT));
            }
            throw new IllegalArgumentException(LoggerMessageFormat.format("{} is not a valid interaction type", s));
        }, INTERACTION_TYPE_FIELD);
    }

    private final Type type;

    public AnalyticsEventInteractionData(Type type) {
        this.type = type;
    }

    public AnalyticsEventInteractionData(StreamInput in) throws IOException {
        this(in.readEnum(Type.class));
    }

    public static AnalyticsEventInteractionData fromXContent(XContentParser parser, AnalyticsContext context) throws IOException {
        return PARSER.parse(parser, context);
    }

    public Type type() {
        return type;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        {
            builder.field(INTERACTION_TYPE_FIELD.getPreferredName(), type());
        }
        return builder.endObject();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeEnum(type);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AnalyticsEventInteractionData that = (AnalyticsEventInteractionData) o;
        return type == that.type;
    }

    @Override
    public int hashCode() {
        return Objects.hash(type);
    }
}

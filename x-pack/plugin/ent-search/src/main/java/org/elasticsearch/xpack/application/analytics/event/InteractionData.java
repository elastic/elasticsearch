/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.analytics.event;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Locale;
import java.util.Objects;

public class InteractionData implements Writeable, ToXContentObject {

    enum Type {
        CLICK("click");

        private final String typeName;

        Type(String typeName) {
            this.typeName = typeName;
        }
    }

    public static final ParseField TYPE_FIELD = new ParseField("type");

    public static final ConstructingObjectParser<InteractionData, Void> PARSER = new ConstructingObjectParser<>(
        "event_interaction_data",
        false,
        a -> new InteractionData((Type) a[0])
    );

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), s -> Type.valueOf(s.toUpperCase(Locale.ROOT)), TYPE_FIELD);
    }
    private final Type type;

    public InteractionData(Type type) {
        this.type = type;
    }

    public InteractionData(StreamInput in) throws IOException {
        this(in.readEnum(Type.class));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeEnum(type);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        {
            builder.field(TYPE_FIELD.getPreferredName(), type);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        InteractionData that = (InteractionData) o;
        return Objects.equals(type, that.type);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type);
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.cluster.metadata;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

/**
 * Represents a single view definition, which is simply a name and a query string.
 */
public final class View implements Writeable, ToXContentObject {
    private static final ParseField NAME = new ParseField("name");
    private static final ParseField QUERY = new ParseField("query");

    // Parser that includes the name field (eg. serializing/deserializing the full object)
    static final ConstructingObjectParser<View, Void> PARSER = new ConstructingObjectParser<>(
        "view",
        false,
        (args, ctx) -> new View((String) args[0], (String) args[1])
    );

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), NAME);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), QUERY);
    }

    // Parser that excludes the name field (eg. when the name is provided externally, in the URL path)
    public static ConstructingObjectParser<View, Void> parser(String name) {
        ConstructingObjectParser<View, Void> parser = new ConstructingObjectParser<>(
            "view",
            false,
            (args, ctx) -> new View(name, (String) args[0])
        );
        parser.declareString(ConstructingObjectParser.constructorArg(), QUERY);
        return parser;
    }

    private final String name;
    private final String query;

    public View(String name, String query) {
        this.name = Objects.requireNonNull(name, "view name must not be null");
        this.query = Objects.requireNonNull(query, "view query must not be null");
    }

    public View(StreamInput in) throws IOException {
        this.name = in.readString();
        this.query = in.readString();
    }

    public static View fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeString(query);
    }

    public String name() {
        return name;
    }

    public String query() {
        return query;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(NAME.getPreferredName(), name);
        builder.field(QUERY.getPreferredName(), query);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        View other = (View) o;
        return Objects.equals(name, other.name) && Objects.equals(query, other.query);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, query);
    }

    public String toString() {
        return Strings.toString(this);
    }
}

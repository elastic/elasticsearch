/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.view;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * Represents an enrich policy including its configuration.
 */
public final class View implements Writeable, ToXContentFragment {
    private static final ParseField QUERY = new ParseField("query");

    static final ConstructingObjectParser<View, Void> PARSER = new ConstructingObjectParser<>(
        "view",
        false,
        (args, ctx) -> new View((String) args[0])
    );

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), QUERY);
    }

    private final String query;

    public View(String query) {
        this.query = query;
    }

    public View(StreamInput in) throws IOException {
        this.query = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(query);
    }

    public String query() {
        return query;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(QUERY.getPreferredName(), query);
        return builder.endObject();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        View policy = (View) o;
        return Objects.equals(query, policy.query);
    }

    @Override
    public int hashCode() {
        return Objects.hash(query);
    }

    public String toString() {
        return Strings.toString(this);
    }
}

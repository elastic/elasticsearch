/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.search;

import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

public class WeightedToken implements Writeable, ToXContentFragment {

    private final String token;
    private final float weight;

    public WeightedToken(String token, float weight) {
        this.token = token;
        this.weight = weight;
    }

    public WeightedToken(StreamInput in) throws IOException {
        this(in.readString(), in.readFloat());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(token);
        out.writeFloat(weight);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(token, weight);
        return builder;
    }

    public Map<String, Object> asMap() {
        return Map.of(token, weight);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    public String token() {
        return token;
    }

    public float weight() {
        return weight;
    }

    private static final ConstructingObjectParser<WeightedToken, String> PARSER = new ConstructingObjectParser<>(
        "query_vector",
        a -> new WeightedToken((String) a[0], (float) a[1])
    );

    public static WeightedToken fromXContent(XContentParser parser) {
        try {
            return PARSER.apply(parser, null);
        } catch (IllegalArgumentException e) {
            throw new ParsingException(parser.getTokenLocation(), e.getMessage(), e);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        WeightedToken that = (WeightedToken) o;
        return Float.compare(weight, that.weight) == 0 && Objects.equals(token, that.token);
    }

    @Override
    public int hashCode() {
        return Objects.hash(token, weight);
    }
}

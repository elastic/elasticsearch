/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.dataframe.transforms.pivot;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.AbstractObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.support.ValueType;

import java.io.IOException;
import java.util.Locale;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

public abstract class SingleGroupSource<AB extends SingleGroupSource<AB>> implements Writeable, ToXContentObject {

    public enum Type {
        TERMS(0);

        private final byte id;

        Type(int id) {
            this.id = (byte) id;
        }

        public byte getId() {
            return id;
        }

        public String value() {
            return name().toLowerCase(Locale.ROOT);
        }
    }

    private static final ParseField FIELD = new ParseField("field");

    // TODO: add script
    private final String field;

    static <VB extends SingleGroupSource<?>, T> void declareValuesSourceFields(AbstractObjectParser<VB, T> parser,
            ValueType targetValueType) {
        // either script or field
        parser.declareString(optionalConstructorArg(), FIELD);
    }

    public SingleGroupSource(final String field) {
        this.field = field;
    }

    public SingleGroupSource(StreamInput in) throws IOException {
        field = in.readOptionalString();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (field != null) {
            builder.field(FIELD.getPreferredName(), field);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalString(field);
    }

    public String getField() {
        return field;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        final SingleGroupSource<?> that = (SingleGroupSource<?>) other;

        return Objects.equals(this.field, that.field);
    }

    @Override
    public int hashCode() {
        return Objects.hash(field);
    }
}

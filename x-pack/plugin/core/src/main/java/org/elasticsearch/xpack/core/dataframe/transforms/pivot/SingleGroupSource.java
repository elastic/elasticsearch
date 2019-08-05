/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.dataframe.transforms.pivot;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.AbstractObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilder;

import java.io.IOException;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

/*
 * Base class for a single source for group_by
 */
public abstract class SingleGroupSource implements Writeable, ToXContentObject {

    public enum Type {
        TERMS(0),
        HISTOGRAM(1),
        DATE_HISTOGRAM(2);

        private final byte id;

        Type(int id) {
            this.id = (byte) id;
        }

        public byte getId() {
            return id;
        }

        public static Type fromId(byte id) {
            switch (id) {
            case 0:
                return TERMS;
            case 1:
                return HISTOGRAM;
            case 2:
                return DATE_HISTOGRAM;
            default:
                throw new IllegalArgumentException("unknown type");
            }
        }

        public String value() {
            return name().toLowerCase(Locale.ROOT);
        }
    }

    protected static final ParseField FIELD = new ParseField("field");

    // TODO: add script
    protected final String field;

    static <T> void declareValuesSourceFields(AbstractObjectParser<? extends SingleGroupSource, T> parser) {
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

    public abstract Type getType();

    public abstract boolean supportsIncrementalBucketUpdate();

    public abstract QueryBuilder getIncrementalBucketUpdateFilterQuery(Set<String> changedBuckets);

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

        final SingleGroupSource that = (SingleGroupSource) other;

        return Objects.equals(this.field, that.field);
    }

    @Override
    public int hashCode() {
        return Objects.hash(field);
    }

    @Override
    public String toString() {
        return Strings.toString(this, true, true);
    }
}

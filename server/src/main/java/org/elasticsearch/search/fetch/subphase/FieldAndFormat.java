/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.fetch.subphase;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

/**
 * Wrapper around a field name and the format that should be used to
 * display values of this field.
 */
public final class FieldAndFormat implements Writeable, ToXContentObject {
    private static final ParseField FIELD_FIELD = new ParseField("field");
    private static final ParseField FORMAT_FIELD = new ParseField("format");
    private static final ParseField INCLUDE_UNMAPPED_FIELD = new ParseField("include_unmapped");

    private static final ConstructingObjectParser<FieldAndFormat, Void> PARSER = new ConstructingObjectParser<>(
        "fetch_field_and_format",
        a -> new FieldAndFormat((String) a[0], (String) a[1], (Boolean) a[2])
    );

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), FIELD_FIELD);
        PARSER.declareStringOrNull(ConstructingObjectParser.optionalConstructorArg(), FORMAT_FIELD);
        PARSER.declareBoolean(ConstructingObjectParser.optionalConstructorArg(), INCLUDE_UNMAPPED_FIELD);
    }

    /**
     * Parse a {@link FieldAndFormat} from some {@link XContent}.
     */
    public static FieldAndFormat fromXContent(XContentParser parser) throws IOException {
        XContentParser.Token token = parser.currentToken();
        if (token.isValue()) {
            return new FieldAndFormat(parser.text(), null);
        } else {
            return PARSER.apply(parser, null);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(FIELD_FIELD.getPreferredName(), field);
        if (format != null) {
            builder.field(FORMAT_FIELD.getPreferredName(), format);
        }
        if (this.includeUnmapped != null) {
            builder.field(INCLUDE_UNMAPPED_FIELD.getPreferredName(), includeUnmapped);
        }
        builder.endObject();
        return builder;
    }

    /** The name of the field. */
    public final String field;

    /** The format of the field, or {@code null} if defaults should be used. */
    public final String format;

    /** Whether to include unmapped fields or not. */
    public final Boolean includeUnmapped;

    public FieldAndFormat(String field, @Nullable String format) {
        this(field, format, null);
    }

    public FieldAndFormat(String field, @Nullable String format, @Nullable Boolean includeUnmapped) {
        this.field = Objects.requireNonNull(field);
        this.format = format;
        this.includeUnmapped = includeUnmapped;
    }

    /** Serialization constructor. */
    public FieldAndFormat(StreamInput in) throws IOException {
        this.field = in.readString();
        if (in.getVersion().onOrAfter(Version.V_6_4_0)) {
            format = in.readOptionalString();
        } else {
            format = null;
        }
        if (in.getVersion().onOrAfter(Version.V_7_11_0)) {
            this.includeUnmapped = in.readOptionalBoolean();
        } else {
            this.includeUnmapped = null;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(field);
        if (out.getVersion().onOrAfter(Version.V_6_4_0)) {
            out.writeOptionalString(format);
        }
        if (out.getVersion().onOrAfter(Version.V_7_11_0)) {
            out.writeOptionalBoolean(this.includeUnmapped);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FieldAndFormat that = (FieldAndFormat) o;
        return Objects.equals(field, that.field)
            && Objects.equals(format, that.format)
            && Objects.equals(includeUnmapped, that.includeUnmapped);
    }

    @Override
    public int hashCode() {
        int h = field.hashCode();
        h = 31 * h + Objects.hashCode(format);
        h = 31 * h + Objects.hashCode(includeUnmapped);
        return h;
    }
}

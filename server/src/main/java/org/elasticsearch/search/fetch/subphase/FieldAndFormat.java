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
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.core.RestApiVersion.equalTo;
import static org.elasticsearch.core.RestApiVersion.onOrAfter;

/**
 * Wrapper around a field name and the format that should be used to
 * display values of this field.
 */
public final class FieldAndFormat implements Writeable, ToXContentObject {
    private static final String USE_DEFAULT_FORMAT = "use_field_mapping";
    private static final DeprecationLogger DEPRECATION_LOGGER = DeprecationLogger.getLogger(FetchDocValuesPhase.class);

    public static final ParseField FIELD_FIELD = new ParseField("field");
    public static final ParseField FORMAT_FIELD = new ParseField("format");
    public static final ParseField INCLUDE_UNMAPPED_FIELD = new ParseField("include_unmapped");

    private static final ConstructingObjectParser<FieldAndFormat, Void> PARSER = new ConstructingObjectParser<>(
        "fetch_field_and_format",
        a -> new FieldAndFormat((String) a[0], (String) a[1], (Boolean) a[2])
    );

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), FIELD_FIELD);
        PARSER.declareStringOrNull(
            ConstructingObjectParser.optionalConstructorArg(),
            FORMAT_FIELD.forRestApiVersion(onOrAfter(RestApiVersion.V_8))
        );
        PARSER.declareField(
            ConstructingObjectParser.optionalConstructorArg(),
            ignoreUseFieldMappingStringParser(),
            FORMAT_FIELD.forRestApiVersion(equalTo(RestApiVersion.V_7)),
            ObjectParser.ValueType.STRING_OR_NULL
        );
        PARSER.declareBoolean(ConstructingObjectParser.optionalConstructorArg(), INCLUDE_UNMAPPED_FIELD);
    }

    private static CheckedFunction<XContentParser, String, IOException> ignoreUseFieldMappingStringParser() {
        return (p) -> {
            if (p.currentToken() == XContentParser.Token.VALUE_NULL) {
                return null;
            } else {
                String text = p.text();
                if (text.equals(USE_DEFAULT_FORMAT)) {
                    DEPRECATION_LOGGER.compatibleCritical(
                        "explicit_default_format",
                        "["
                            + USE_DEFAULT_FORMAT
                            + "] is a special format that was only used to "
                            + "ease the transition to 7.x. It has become the default and shouldn't be set explicitly anymore."
                    );
                    return null;
                } else {
                    return text;
                }
            }
        };
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
        format = in.readOptionalString();
        if (in.getVersion().onOrAfter(Version.V_7_11_0)) {
            this.includeUnmapped = in.readOptionalBoolean();
        } else {
            this.includeUnmapped = null;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(field);
        out.writeOptionalString(format);
        if (out.getVersion().onOrAfter(Version.V_7_11_0)) {
            out.writeOptionalBoolean(this.includeUnmapped);
        }
    }

    @Override
    public int hashCode() {
        int h = field.hashCode();
        h = 31 * h + Objects.hashCode(format);
        h = 31 * h + Objects.hashCode(includeUnmapped);
        return h;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        FieldAndFormat other = (FieldAndFormat) obj;
        return field.equals(other.field) && Objects.equals(format, other.format) && Objects.equals(includeUnmapped, other.includeUnmapped);
    }
}

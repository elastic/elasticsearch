/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.job.config;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.MlParserType;

import java.io.IOException;
import java.util.EnumMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

public class FilterRef implements ToXContentObject, Writeable {

    public static final ParseField FILTER_REF_FIELD = new ParseField("filter_ref");
    public static final ParseField FILTER_ID = new ParseField("filter_id");
    public static final ParseField FILTER_TYPE = new ParseField("filter_type");

    public enum FilterType {
        INCLUDE, EXCLUDE;

        public static FilterType fromString(String value) {
            return valueOf(value.toUpperCase(Locale.ROOT));
        }

        @Override
        public String toString() {
            return name().toLowerCase(Locale.ROOT);
        }
    }

    // These parsers follow the pattern that metadata is parsed leniently (to allow for enhancements), whilst config is parsed strictly
    public static final ConstructingObjectParser<FilterRef, Void> METADATA_PARSER =
            new ConstructingObjectParser<>(FILTER_REF_FIELD.getPreferredName(), true,
                    a -> new FilterRef((String) a[0], (FilterType) a[1]));
    public static final ConstructingObjectParser<FilterRef, Void> CONFIG_PARSER =
            new ConstructingObjectParser<>(FILTER_REF_FIELD.getPreferredName(), false,
                    a -> new FilterRef((String) a[0], (FilterType) a[1]));
    public static final Map<MlParserType, ConstructingObjectParser<FilterRef, Void>> PARSERS = new EnumMap<>(MlParserType.class);

    static {
        PARSERS.put(MlParserType.METADATA, METADATA_PARSER);
        PARSERS.put(MlParserType.CONFIG, CONFIG_PARSER);
        for (MlParserType parserType : MlParserType.values()) {
            ConstructingObjectParser<FilterRef, Void> parser = PARSERS.get(parserType);
            assert parser != null;
            parser.declareString(ConstructingObjectParser.constructorArg(), FILTER_ID);
            parser.declareField(ConstructingObjectParser.optionalConstructorArg(), p -> {
                if (p.currentToken() == XContentParser.Token.VALUE_STRING) {
                    return FilterType.fromString(p.text());
                }
                throw new IllegalArgumentException("Unsupported token [" + p.currentToken() + "]");
            }, FILTER_TYPE, ObjectParser.ValueType.STRING);
        }
    }

    private final String filterId;
    private final FilterType filterType;

    public FilterRef(String filterId, FilterType filterType) {
        this.filterId = Objects.requireNonNull(filterId);
        this.filterType = filterType == null ? FilterType.INCLUDE : filterType;
    }

    public FilterRef(StreamInput in) throws IOException {
        filterId = in.readString();
        filterType = in.readEnum(FilterType.class);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(filterId);
        out.writeEnum(filterType);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(FILTER_ID.getPreferredName(), filterId);
        builder.field(FILTER_TYPE.getPreferredName(), filterType);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj instanceof FilterRef == false) {
            return false;
        }

        FilterRef other = (FilterRef) obj;
        return Objects.equals(filterId, other.filterId) && Objects.equals(filterType, other.filterType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(filterId, filterType);
    }

    public String getFilterId() {
        return filterId;
    }

    public FilterType getFilterType() {
        return filterType;
    }
}

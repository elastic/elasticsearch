/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml.job.config;

import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Locale;
import java.util.Objects;

public class FilterRef implements ToXContentObject {

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

    public static final ConstructingObjectParser<FilterRef, Void> PARSER =
        new ConstructingObjectParser<>(FILTER_REF_FIELD.getPreferredName(), true, a -> new FilterRef((String) a[0], (FilterType) a[1]));

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), FILTER_ID);
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), FilterType::fromString, FILTER_TYPE);
    }

    private final String filterId;
    private final FilterType filterType;

    public FilterRef(String filterId, FilterType filterType) {
        this.filterId = Objects.requireNonNull(filterId);
        this.filterType = filterType == null ? FilterType.INCLUDE : filterType;
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

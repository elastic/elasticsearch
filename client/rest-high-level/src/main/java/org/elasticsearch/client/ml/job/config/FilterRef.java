/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.client.ml.job.config;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

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
        PARSER.declareField(ConstructingObjectParser.optionalConstructorArg(), p -> {
            if (p.currentToken() == XContentParser.Token.VALUE_STRING) {
                return FilterType.fromString(p.text());
            }
            throw new IllegalArgumentException("Unsupported token [" + p.currentToken() + "]");
        }, FILTER_TYPE, ObjectParser.ValueType.STRING);
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

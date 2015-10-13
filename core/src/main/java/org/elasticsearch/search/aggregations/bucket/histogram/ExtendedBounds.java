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

package org.elasticsearch.search.aggregations.bucket.histogram;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.rounding.Rounding;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.SearchParseException;
import org.elasticsearch.search.aggregations.support.format.ValueParser;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Objects;

/**
 *
 */
public class ExtendedBounds implements ToXContent {

    static final ParseField EXTENDED_BOUNDS_FIELD = new ParseField("extended_bounds");
    static final ParseField MIN_FIELD = new ParseField("min");
    static final ParseField MAX_FIELD = new ParseField("max");

    private static final ExtendedBounds PROTOTYPE = new ExtendedBounds();

    Long min;
    Long max;

    String minAsStr;
    String maxAsStr;

    ExtendedBounds() {} //for serialization

    public ExtendedBounds(Long min, Long max) {
        this.min = min;
        this.max = max;
    }

    void processAndValidate(String aggName, SearchContext context, ValueParser parser) {
        assert parser != null;
        if (minAsStr != null) {
            min = parser.parseLong(minAsStr, context);
        }
        if (maxAsStr != null) {
            max = parser.parseLong(maxAsStr, context);
        }
        if (min != null && max != null && min.compareTo(max) > 0) {
            throw new SearchParseException(context, "[extended_bounds.min][" + min + "] cannot be greater than " +
                    "[extended_bounds.max][" + max + "] for histogram aggregation [" + aggName + "]", null);
        }
    }

    ExtendedBounds round(Rounding rounding) {
        return new ExtendedBounds(min != null ? rounding.round(min) : null, max != null ? rounding.round(max) : null);
    }

    void writeTo(StreamOutput out) throws IOException {
        if (min != null) {
            out.writeBoolean(true);
            out.writeLong(min);
        } else {
            out.writeBoolean(false);
        }
        if (max != null) {
            out.writeBoolean(true);
            out.writeLong(max);
        } else {
            out.writeBoolean(false);
        }
    }

    static ExtendedBounds readFrom(StreamInput in) throws IOException {
        ExtendedBounds bounds = new ExtendedBounds();
        if (in.readBoolean()) {
            bounds.min = in.readLong();
        }
        if (in.readBoolean()) {
            bounds.max = in.readLong();
        }
        return bounds;
    }

    public ExtendedBounds fromXContent(XContentParser parser, ParseFieldMatcher parseFieldMatcher, String aggregationName)
            throws IOException {
        XContentParser.Token token = null;
        String currentFieldName = null;
        ExtendedBounds extendedBounds = new ExtendedBounds();
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.VALUE_STRING) {
                if ("min".equals(currentFieldName)) {
                    extendedBounds.minAsStr = parser.text();
                } else if ("max".equals(currentFieldName)) {
                    extendedBounds.maxAsStr = parser.text();
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "Unknown extended_bounds key for a " + token
                            + " in aggregation [" + aggregationName + "]: [" + currentFieldName + "].");
                }
            } else if (token == XContentParser.Token.VALUE_NUMBER) {
                if (parseFieldMatcher.match(currentFieldName, MIN_FIELD)) {
                    extendedBounds.min = parser.longValue(true);
                } else if (parseFieldMatcher.match(currentFieldName, MAX_FIELD)) {
                    extendedBounds.max = parser.longValue(true);
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "Unknown extended_bounds key for a " + token
                            + " in aggregation [" + aggregationName + "]: [" + currentFieldName + "].");
                }
            }
        }
        return extendedBounds;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(EXTENDED_BOUNDS_FIELD.getPreferredName());
        if (min != null) {
            builder.field(MIN_FIELD.getPreferredName(), min);
        }
        if (max != null) {
            builder.field(MAX_FIELD.getPreferredName(), max);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(min, max);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        ExtendedBounds other = (ExtendedBounds) obj;
        return Objects.equals(min, other.min)
                && Objects.equals(min, other.min);
    }

    public static ExtendedBounds parse(XContentParser parser, ParseFieldMatcher parseFieldMatcher, String aggregationName)
            throws IOException {
        return PROTOTYPE.fromXContent(parser, parseFieldMatcher, aggregationName);
    }
}

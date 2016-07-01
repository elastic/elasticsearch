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
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.rounding.Rounding;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.SearchParseException;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Objects;

public class ExtendedBounds implements ToXContent, Writeable {

    static final ParseField EXTENDED_BOUNDS_FIELD = new ParseField("extended_bounds");
    static final ParseField MIN_FIELD = new ParseField("min");
    static final ParseField MAX_FIELD = new ParseField("max");

    Long min;
    Long max;

    String minAsStr;
    String maxAsStr;

    ExtendedBounds() {} //for parsing

    public ExtendedBounds(Long min, Long max) {
        this.min = min;
        this.max = max;
    }

    public ExtendedBounds(String minAsStr, String maxAsStr) {
        this.minAsStr = minAsStr;
        this.maxAsStr = maxAsStr;
    }

    /**
     * Read from a stream.
     */
    public ExtendedBounds(StreamInput in) throws IOException {
        if (in.readBoolean()) {
            min = in.readLong();
        }
        if (in.readBoolean()) {
            max = in.readLong();
        }
        minAsStr = in.readOptionalString();
        maxAsStr = in.readOptionalString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
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
        out.writeOptionalString(minAsStr);
        out.writeOptionalString(maxAsStr);
    }


    void processAndValidate(String aggName, SearchContext context, DocValueFormat format) {
        assert format != null;
        if (minAsStr != null) {
            min = format.parseLong(minAsStr, false, context.nowCallable());
        }
        if (maxAsStr != null) {
            // TODO: Should we rather pass roundUp=true?
            max = format.parseLong(maxAsStr, false, context.nowCallable());
        }
        if (min != null && max != null && min.compareTo(max) > 0) {
            throw new SearchParseException(context, "[extended_bounds.min][" + min + "] cannot be greater than " +
                    "[extended_bounds.max][" + max + "] for histogram aggregation [" + aggName + "]", null);
        }
    }

    ExtendedBounds round(Rounding rounding) {
        return new ExtendedBounds(min != null ? rounding.round(min) : null, max != null ? rounding.round(max) : null);
    }

    public static ExtendedBounds fromXContent(XContentParser parser, ParseFieldMatcher parseFieldMatcher, String aggregationName)
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
        } else {
            builder.field(MIN_FIELD.getPreferredName(), minAsStr);
        }
        if (max != null) {
            builder.field(MAX_FIELD.getPreferredName(), max);
        } else {
            builder.field(MAX_FIELD.getPreferredName(), maxAsStr);
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
}

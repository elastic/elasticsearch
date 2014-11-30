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

package org.elasticsearch.search.reducers.bucket.range;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.SearchParseException;
import org.elasticsearch.search.aggregations.bucket.range.RangeAggregator;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.reducers.Reducer;
import org.elasticsearch.search.reducers.ReducerFactory;

import java.io.IOException;
import java.util.ArrayList;

public class RangeParser implements Reducer.Parser{

    protected static final ParseField FIELD_NAME_FIELD = new ParseField("field");
    protected static final ParseField BUCKETS_FIELD = new ParseField("buckets");
    public static final String[] TYPES = {InternalRange.TYPE.name()};

    @Override
    public String[] types() {
        return TYPES;
    }

    @Override
    public ReducerFactory parse(String reducerName, XContentParser parser, SearchContext context) throws IOException {

        String buckets = null;
        String fieldName = null;
        ArrayList<RangeAggregator.Range> ranges = null;

        XContentParser.Token token;
        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_ARRAY) {
                if ("ranges".equals(currentFieldName)) {
                    ranges = new ArrayList<>();
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        double from = Double.NEGATIVE_INFINITY;
                        String fromAsStr = null;
                        double to = Double.POSITIVE_INFINITY;
                        String toAsStr = null;
                        String key = null;
                        String toOrFromOrKey = null;
                        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                            if (token == XContentParser.Token.FIELD_NAME) {
                                toOrFromOrKey = parser.currentName();
                            } else if (token == XContentParser.Token.VALUE_NUMBER) {
                                if ("from".equals(toOrFromOrKey)) {
                                    from = parser.doubleValue();
                                } else if ("to".equals(toOrFromOrKey)) {
                                    to = parser.doubleValue();
                                }
                            } else if (token == XContentParser.Token.VALUE_STRING) {
                                if ("from".equals(toOrFromOrKey)) {
                                    fromAsStr = parser.text();
                                } else if ("to".equals(toOrFromOrKey)) {
                                    toAsStr = parser.text();
                                } else if ("key".equals(toOrFromOrKey)) {
                                    key = parser.text();
                                }
                            }
                        }
                        ranges.add(new RangeAggregator.Range(key, from, fromAsStr, to, toAsStr));
                    }
                } else {
                    throw new SearchParseException(context, "Unknown key for a " + token + " in [" + reducerName + "]: [" + currentFieldName + "].");
                }


            } else if (token.isValue()) {
                if (token == XContentParser.Token.VALUE_STRING) {
                    if (BUCKETS_FIELD.match(currentFieldName)) {
                        buckets = parser.text();
                    } else if (FIELD_NAME_FIELD.match(currentFieldName)) {
                        fieldName = parser.text();
                    } else {
                        throw new SearchParseException(context, "Unknown key for a " + token + " in [" + reducerName + "]: [" + currentFieldName + "].");
                    }
                } else {
                    throw new SearchParseException(context, "Unknown key for a " + token + " in [" + reducerName + "]: [" + currentFieldName + "].");
                }
            } else {
                throw new SearchParseException(context, "Unknown key for a " + token + " in [" + reducerName + "]: [" + currentFieldName + "].");
            }
        }

        if (buckets == null) {
            throw new SearchParseException(context, "Missing [" + BUCKETS_FIELD.getPreferredName() + "] in " + TYPES[0] + " reducer [" + reducerName + "]");
        }

        if (ranges == null) {
            throw new SearchParseException(context, "Missing [ranges] in ranges reducer [" + reducerName + "]");
        }

        return new RangeReducer.Factory(reducerName, buckets, fieldName, ranges);
    }

}

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

package org.elasticsearch.search.aggregations.bucket.filters;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.ParsedQuery;
import org.elasticsearch.search.SearchParseException;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class FiltersParser implements Aggregator.Parser {

    public static final ParseField FILTERS_FIELD = new ParseField("filters");
    public static final ParseField OTHER_BUCKET_FIELD = new ParseField("other_bucket");
    public static final ParseField OTHER_BUCKET_KEY_FIELD = new ParseField("other_bucket_key");

    @Override
    public String type() {
        return InternalFilters.TYPE.name();
    }

    @Override
    public AggregatorFactory parse(String aggregationName, XContentParser parser, SearchContext context) throws IOException {

        List<FiltersAggregator.KeyedFilter> filters = new ArrayList<>();

        XContentParser.Token token = null;
        String currentFieldName = null;
        Boolean keyed = null;
        String otherBucketKey = null;
        boolean otherBucket = false;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.VALUE_BOOLEAN) {
                if (context.parseFieldMatcher().match(currentFieldName, OTHER_BUCKET_FIELD)) {
                    otherBucket = parser.booleanValue();
                } else {
                    throw new SearchParseException(context, "Unknown key for a " + token + " in [" + aggregationName + "]: ["
                            + currentFieldName + "].", parser.getTokenLocation());
                }
            } else if (token == XContentParser.Token.VALUE_STRING) {
                if (context.parseFieldMatcher().match(currentFieldName, OTHER_BUCKET_KEY_FIELD)) {
                    otherBucketKey = parser.text();
                    otherBucket = true;
                } else {
                    throw new SearchParseException(context, "Unknown key for a " + token + " in [" + aggregationName + "]: ["
                            + currentFieldName + "].", parser.getTokenLocation());
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (context.parseFieldMatcher().match(currentFieldName, FILTERS_FIELD)) {
                    keyed = true;
                    String key = null;
                    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                        if (token == XContentParser.Token.FIELD_NAME) {
                            key = parser.currentName();
                        } else {
                            ParsedQuery filter = context.queryParserService().parseInnerFilter(parser);
                            filters.add(new FiltersAggregator.KeyedFilter(key, filter == null ? Queries.newMatchAllQuery() : filter.query()));
                        }
                    }
                } else {
                    throw new SearchParseException(context, "Unknown key for a " + token + " in [" + aggregationName + "]: ["
                            + currentFieldName + "].", parser.getTokenLocation());
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                if (context.parseFieldMatcher().match(currentFieldName, FILTERS_FIELD)) {
                    keyed = false;
                    int idx = 0;
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        ParsedQuery filter = context.queryParserService().parseInnerFilter(parser);
                        filters.add(new FiltersAggregator.KeyedFilter(String.valueOf(idx), filter == null ? Queries.newMatchAllQuery()
                                : filter.query()));
                        idx++;
                    }
                } else {
                    throw new SearchParseException(context, "Unknown key for a " + token + " in [" + aggregationName + "]: ["
                            + currentFieldName + "].", parser.getTokenLocation());
                }
            } else {
                throw new SearchParseException(context, "Unknown key for a " + token + " in [" + aggregationName + "]: ["
                        + currentFieldName + "].", parser.getTokenLocation());
            }
        }

        if (otherBucket && otherBucketKey == null) {
            otherBucketKey = "_other_";
        }

        return new FiltersAggregator.Factory(aggregationName, filters, keyed, otherBucketKey);
    }

}

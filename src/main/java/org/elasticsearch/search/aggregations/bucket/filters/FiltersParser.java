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

import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.ParsedFilter;
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
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                if ("filters".equals(currentFieldName)) {
                    keyed = true;
                    String key = null;
                    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                        if (token == XContentParser.Token.FIELD_NAME) {
                            key = parser.currentName();
                        } else {
                            ParsedFilter filter = context.queryParserService().parseInnerFilter(parser);
                            filters.add(new FiltersAggregator.KeyedFilter(key, filter == null ? Queries.newMatchAllFilter() : filter.filter()));
                        }
                    }
                } else {
                    throw new SearchParseException(context, "Unknown key for a " + token + " in [" + aggregationName + "]: [" + currentFieldName + "].");
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                if ("filters".equals(currentFieldName)) {
                    keyed = false;
                    int idx = 0;
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        ParsedFilter filter = context.queryParserService().parseInnerFilter(parser);
                        filters.add(new FiltersAggregator.KeyedFilter(String.valueOf(idx), filter == null ? Queries.newMatchAllFilter()
                                : filter.filter()));
                        idx++;
                    }
                } else {
                    throw new SearchParseException(context, "Unknown key for a " + token + " in [" + aggregationName + "]: [" + currentFieldName + "].");
                }
            } else {
                throw new SearchParseException(context, "Unknown key for a " + token + " in [" + aggregationName + "]: [" + currentFieldName + "].");
            }
        }

        return new FiltersAggregator.Factory(aggregationName, filters, keyed);
    }

}

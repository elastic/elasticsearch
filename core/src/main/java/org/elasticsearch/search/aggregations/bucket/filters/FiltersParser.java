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
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.indices.query.IndicesQueriesRegistry;
import org.elasticsearch.search.aggregations.Aggregator;
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
    private final IndicesQueriesRegistry queriesRegistry;

    @Inject
    public FiltersParser(IndicesQueriesRegistry queriesRegistry) {
        this.queriesRegistry = queriesRegistry;
    }

    @Override
    public String type() {
        return InternalFilters.TYPE.name();
    }

    @Override
    public FiltersAggregatorBuilder parse(String aggregationName, QueryParseContext context)
            throws IOException {
        XContentParser parser = context.parser();

        List<FiltersAggregator.KeyedFilter> keyedFilters = null;
        List<QueryBuilder<?>> nonKeyedFilters = null;

        XContentParser.Token token = null;
        String currentFieldName = null;
        String otherBucketKey = null;
        Boolean otherBucket = false;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.VALUE_BOOLEAN) {
                if (context.parseFieldMatcher().match(currentFieldName, OTHER_BUCKET_FIELD)) {
                    otherBucket = parser.booleanValue();
                } else {
                    throw new ParsingException(parser.getTokenLocation(),
                            "Unknown key for a " + token + " in [" + aggregationName + "]: [" + currentFieldName + "].");
                }
            } else if (token == XContentParser.Token.VALUE_STRING) {
                if (context.parseFieldMatcher().match(currentFieldName, OTHER_BUCKET_KEY_FIELD)) {
                    otherBucketKey = parser.text();
                } else {
                    throw new ParsingException(parser.getTokenLocation(),
                            "Unknown key for a " + token + " in [" + aggregationName + "]: [" + currentFieldName + "].");
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (context.parseFieldMatcher().match(currentFieldName, FILTERS_FIELD)) {
                    keyedFilters = new ArrayList<>();
                    String key = null;
                    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                        if (token == XContentParser.Token.FIELD_NAME) {
                            key = parser.currentName();
                        } else {
                            QueryParseContext queryParseContext = new QueryParseContext(queriesRegistry);
                            queryParseContext.reset(parser);
                            queryParseContext.parseFieldMatcher(context.parseFieldMatcher());
                            QueryBuilder<?> filter = queryParseContext.parseInnerQueryBuilder();
                            keyedFilters
                                    .add(new FiltersAggregator.KeyedFilter(key, filter == null ? QueryBuilders.matchAllQuery() : filter));
                        }
                    }
                } else {
                    throw new ParsingException(parser.getTokenLocation(),
                            "Unknown key for a " + token + " in [" + aggregationName + "]: [" + currentFieldName + "].");
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                if (context.parseFieldMatcher().match(currentFieldName, FILTERS_FIELD)) {
                    nonKeyedFilters = new ArrayList<>();
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        QueryParseContext queryParseContext = new QueryParseContext(queriesRegistry);
                        queryParseContext.reset(parser);
                        queryParseContext.parseFieldMatcher(context.parseFieldMatcher());
                        QueryBuilder<?> filter = queryParseContext.parseInnerQueryBuilder();
                        nonKeyedFilters.add(filter == null ? QueryBuilders.matchAllQuery() : filter);
                    }
                } else {
                    throw new ParsingException(parser.getTokenLocation(),
                            "Unknown key for a " + token + " in [" + aggregationName + "]: [" + currentFieldName + "].");
                }
            } else {
                throw new ParsingException(parser.getTokenLocation(),
                        "Unknown key for a " + token + " in [" + aggregationName + "]: [" + currentFieldName + "].");
            }
        }

        if (otherBucket && otherBucketKey == null) {
            otherBucketKey = "_other_";
        }

        FiltersAggregatorBuilder factory;
        if (keyedFilters != null) {
            factory = new FiltersAggregatorBuilder(aggregationName,
                    keyedFilters.toArray(new FiltersAggregator.KeyedFilter[keyedFilters.size()]));
        } else {
            factory = new FiltersAggregatorBuilder(aggregationName,
                    nonKeyedFilters.toArray(new QueryBuilder<?>[nonKeyedFilters.size()]));
        }
        if (otherBucket != null) {
            factory.otherBucket(otherBucket);
        }
        if (otherBucketKey != null) {
            factory.otherBucketKey(otherBucketKey);
        }
        return factory;
    }

    @Override
    public FiltersAggregatorBuilder getFactoryPrototypes() {
        return FiltersAggregatorBuilder.PROTOTYPE;
    }

}

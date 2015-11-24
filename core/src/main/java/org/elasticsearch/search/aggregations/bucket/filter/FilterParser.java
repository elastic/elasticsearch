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
package org.elasticsearch.search.aggregations.bucket.filter;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.indices.query.IndicesQueriesRegistry;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;

/**
 *
 */
public class FilterParser implements Aggregator.Parser {

    private IndicesQueriesRegistry queriesRegistry;

    @Inject
    public FilterParser(IndicesQueriesRegistry queriesRegistry) {
        this.queriesRegistry = queriesRegistry;
    }

    @Override
    public String type() {
        return InternalFilter.TYPE.name();
    }

    @Override
    public AggregatorFactory parse(String aggregationName, XContentParser parser, SearchContext context) throws IOException {
        QueryParseContext queryParseContext = new QueryParseContext(queriesRegistry);
        queryParseContext.reset(parser);
        queryParseContext.parseFieldMatcher(context.parseFieldMatcher());
        QueryBuilder<?> filter = queryParseContext.parseInnerQueryBuilder();

        FilterAggregator.Factory factory = new FilterAggregator.Factory(aggregationName);
        factory.filter(filter == null ? new MatchAllQueryBuilder() : filter);
        return factory;
    }

    // NORELEASE implement this method when refactoring this aggregation
    @Override
    public AggregatorFactory[] getFactoryPrototypes() {
        return new AggregatorFactory[] { new FilterAggregator.Factory(null) };
    }

}

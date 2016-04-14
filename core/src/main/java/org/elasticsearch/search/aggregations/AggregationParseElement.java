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
package org.elasticsearch.search.aggregations;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.indices.query.IndicesQueriesRegistry;
import org.elasticsearch.search.SearchParseElement;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.internal.SearchContext;

/**
 * The search parse element that is responsible for parsing the get part of the request.
 *
 * For example (in bold):
 * <pre>
 *      curl -XGET 'localhost:9200/_search?search_type=count' -d '{
 *          query: {
 *              match_all : {}
 *          },
 *          addAggregation : {
 *              avg_price: {
 *                  avg : { field : price }
 *              },
 *              categories: {
 *                  terms : { field : category, size : 12 },
 *                  addAggregation: {
 *                      avg_price : { avg : { field : price }}
 *                  }
 *              }
 *          }
 *      }'
 * </pre>
 */
public class AggregationParseElement implements SearchParseElement {

    private final AggregatorParsers aggregatorParsers;
    private IndicesQueriesRegistry queriesRegistry;

    @Inject
    public AggregationParseElement(AggregatorParsers aggregatorParsers, IndicesQueriesRegistry queriesRegistry) {
        this.aggregatorParsers = aggregatorParsers;
        this.queriesRegistry = queriesRegistry;
    }

    @Override
    public void parse(XContentParser parser, SearchContext context) throws Exception {
        QueryParseContext parseContext = new QueryParseContext(queriesRegistry);
        parseContext.reset(parser);
        parseContext.parseFieldMatcher(context.parseFieldMatcher());
        AggregatorFactories.Builder builders = aggregatorParsers.parseAggregators(parseContext);
        AggregationContext aggContext = new AggregationContext(context);
        AggregatorFactories factories = builders.build(aggContext, null);
        factories.validate();
        context.aggregations(new SearchContextAggregations(factories));
    }
}

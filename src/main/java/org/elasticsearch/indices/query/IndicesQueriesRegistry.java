/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.indices.query;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.*;

import java.util.Map;

/**
 *
 */
public class IndicesQueriesRegistry {

    private ImmutableMap<String, QueryParser> queryParsers;
    private ImmutableMap<String, FilterParser> filterParsers;

    @Inject
    public IndicesQueriesRegistry(Settings settings, @Nullable ClusterService clusterService) {
        Map<String, QueryParser> queryParsers = Maps.newHashMap();
        addQueryParser(queryParsers, new TextQueryParser());
        addQueryParser(queryParsers, new NestedQueryParser());
        addQueryParser(queryParsers, new HasChildQueryParser());
        addQueryParser(queryParsers, new TopChildrenQueryParser());
        addQueryParser(queryParsers, new DisMaxQueryParser());
        addQueryParser(queryParsers, new IdsQueryParser());
        addQueryParser(queryParsers, new MatchAllQueryParser());
        addQueryParser(queryParsers, new QueryStringQueryParser(settings));
        addQueryParser(queryParsers, new BoostingQueryParser());
        addQueryParser(queryParsers, new BoolQueryParser(settings));
        addQueryParser(queryParsers, new TermQueryParser());
        addQueryParser(queryParsers, new TermsQueryParser());
        addQueryParser(queryParsers, new FuzzyQueryParser());
        addQueryParser(queryParsers, new FieldQueryParser(settings));
        addQueryParser(queryParsers, new RangeQueryParser());
        addQueryParser(queryParsers, new PrefixQueryParser());
        addQueryParser(queryParsers, new WildcardQueryParser());
        addQueryParser(queryParsers, new FilteredQueryParser());
        addQueryParser(queryParsers, new ConstantScoreQueryParser());
        addQueryParser(queryParsers, new CustomBoostFactorQueryParser());
        addQueryParser(queryParsers, new CustomScoreQueryParser());
        addQueryParser(queryParsers, new CustomFiltersScoreQueryParser());
        addQueryParser(queryParsers, new SpanTermQueryParser());
        addQueryParser(queryParsers, new SpanNotQueryParser());
        addQueryParser(queryParsers, new SpanFirstQueryParser());
        addQueryParser(queryParsers, new SpanNearQueryParser());
        addQueryParser(queryParsers, new SpanOrQueryParser());
        addQueryParser(queryParsers, new MoreLikeThisQueryParser());
        addQueryParser(queryParsers, new MoreLikeThisFieldQueryParser());
        addQueryParser(queryParsers, new FuzzyLikeThisQueryParser());
        addQueryParser(queryParsers, new FuzzyLikeThisFieldQueryParser());
        addQueryParser(queryParsers, new WrapperQueryParser());
        addQueryParser(queryParsers, new IndicesQueryParser(clusterService));
        this.queryParsers = ImmutableMap.copyOf(queryParsers);

        Map<String, FilterParser> filterParsers = Maps.newHashMap();
        addFilterParser(filterParsers, new HasChildFilterParser());
        addFilterParser(filterParsers, new NestedFilterParser());
        addFilterParser(filterParsers, new TypeFilterParser());
        addFilterParser(filterParsers, new IdsFilterParser());
        addFilterParser(filterParsers, new LimitFilterParser());
        addFilterParser(filterParsers, new TermFilterParser());
        addFilterParser(filterParsers, new TermsFilterParser());
        addFilterParser(filterParsers, new RangeFilterParser());
        addFilterParser(filterParsers, new NumericRangeFilterParser());
        addFilterParser(filterParsers, new PrefixFilterParser());
        addFilterParser(filterParsers, new ScriptFilterParser());
        addFilterParser(filterParsers, new GeoDistanceFilterParser());
        addFilterParser(filterParsers, new GeoDistanceRangeFilterParser());
        addFilterParser(filterParsers, new GeoBoundingBoxFilterParser());
        addFilterParser(filterParsers, new GeoPolygonFilterParser());
        addFilterParser(filterParsers, new QueryFilterParser());
        addFilterParser(filterParsers, new FQueryFilterParser());
        addFilterParser(filterParsers, new BoolFilterParser());
        addFilterParser(filterParsers, new AndFilterParser());
        addFilterParser(filterParsers, new OrFilterParser());
        addFilterParser(filterParsers, new NotFilterParser());
        addFilterParser(filterParsers, new MatchAllFilterParser());
        addFilterParser(filterParsers, new ExistsFilterParser());
        addFilterParser(filterParsers, new MissingFilterParser());
        this.filterParsers = ImmutableMap.copyOf(filterParsers);
    }

    /**
     * Adds a global query parser.
     */
    public void addQueryParser(QueryParser queryParser) {
        Map<String, QueryParser> queryParsers = Maps.newHashMap(this.queryParsers);
        addQueryParser(queryParsers, queryParser);
        this.queryParsers = ImmutableMap.copyOf(queryParsers);
    }

    public void addFilterParser(FilterParser filterParser) {
        Map<String, FilterParser> filterParsers = Maps.newHashMap(this.filterParsers);
        addFilterParser(filterParsers, filterParser);
        this.filterParsers = ImmutableMap.copyOf(filterParsers);
    }

    public ImmutableMap<String, QueryParser> queryParsers() {
        return queryParsers;
    }

    public ImmutableMap<String, FilterParser> filterParsers() {
        return filterParsers;
    }

    private void addQueryParser(Map<String, QueryParser> queryParsers, QueryParser queryParser) {
        for (String name : queryParser.names()) {
            queryParsers.put(name, queryParser);
        }
    }

    private void addFilterParser(Map<String, FilterParser> filterParsers, FilterParser filterParser) {
        for (String name : filterParser.names()) {
            filterParsers.put(name, filterParser);
        }
    }
}
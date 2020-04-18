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

package org.elasticsearch.search.aggregations.support;

import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public abstract class ArrayValuesSourceAggregatorFactory
    extends AggregatorFactory {

    protected Map<String, ValuesSourceConfig> configs;

    public ArrayValuesSourceAggregatorFactory(String name, Map<String, ValuesSourceConfig> configs,
                                              QueryShardContext queryShardContext, AggregatorFactory parent,
                                              AggregatorFactories.Builder subFactoriesBuilder,
                                              Map<String, Object> metadata) throws IOException {
        super(name, queryShardContext, parent, subFactoriesBuilder, metadata);
        this.configs = configs;
    }

    @Override
    public Aggregator createInternal(SearchContext searchContext,
                                        Aggregator parent,
                                        boolean collectsFromSingleBucket,
                                        Map<String, Object> metadata) throws IOException {
        HashMap<String, ValuesSource> valuesSources = new HashMap<>();

        for (Map.Entry<String, ValuesSourceConfig> config : configs.entrySet()) {
            ValuesSource vs = config.getValue().toValuesSource();
            if (vs != null) {
                valuesSources.put(config.getKey(), vs);
            }
        }
        if (valuesSources.isEmpty()) {
            return createUnmapped(searchContext, parent, metadata);
        }
        return doCreateInternal(valuesSources, searchContext, parent, collectsFromSingleBucket, metadata);
    }

    protected abstract Aggregator createUnmapped(SearchContext searchContext,
                                                    Aggregator parent,
                                                    Map<String, Object> metadata) throws IOException;

    protected abstract Aggregator doCreateInternal(Map<String, ValuesSource> valuesSources,
                                                    SearchContext searchContext,
                                                    Aggregator parent,
                                                    boolean collectsFromSingleBucket,
                                                    Map<String, Object> metadata) throws IOException;

}

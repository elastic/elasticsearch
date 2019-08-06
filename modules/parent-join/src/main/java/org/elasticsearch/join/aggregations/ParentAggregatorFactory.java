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

package org.elasticsearch.join.aggregations;

import org.apache.lucene.search.Query;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.NonCollectingAggregator;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.ValuesSource.Bytes.WithOrdinals;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class ParentAggregatorFactory extends ValuesSourceAggregatorFactory<WithOrdinals> {

    private final Query parentFilter;
    private final Query childFilter;

    public ParentAggregatorFactory(String name,
                                   ValuesSourceConfig<WithOrdinals> config,
                                   Query childFilter,
                                   Query parentFilter,
                                   SearchContext context,
                                   AggregatorFactory parent,
                                   AggregatorFactories.Builder subFactoriesBuilder,
                                   Map<String, Object> metaData) throws IOException {
        super(name, config, context, parent, subFactoriesBuilder, metaData);

        this.childFilter = childFilter;
        this.parentFilter = parentFilter;
    }

    @Override
    protected Aggregator createUnmapped(Aggregator parent,
                                        List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) throws IOException {
        return new NonCollectingAggregator(name, context, parent, pipelineAggregators, metaData) {
            @Override
            public InternalAggregation buildEmptyAggregation() {
                return new InternalParent(name, 0, buildEmptySubAggregations(), pipelineAggregators(), metaData());
            }
        };
    }

    @Override
    protected Aggregator doCreateInternal(WithOrdinals valuesSource,
                                          Aggregator children,
                                          boolean collectsFromSingleBucket,
                                          List<PipelineAggregator> pipelineAggregators,
                                          Map<String, Object> metaData) throws IOException {

        long maxOrd = valuesSource.globalMaxOrd(context.searcher());
        if (collectsFromSingleBucket) {
            return new ChildrenToParentAggregator(name, factories, context, children, childFilter,
                parentFilter, valuesSource, maxOrd, pipelineAggregators, metaData);
        } else {
            return asMultiBucketAggregator(this, context, children);
        }
    }
}

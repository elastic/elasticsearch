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

package org.elasticsearch.search.aggregations.bucket.range;

import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.bucket.range.RangeAggregator.Range;
import org.elasticsearch.search.aggregations.bucket.range.RangeAggregator.Unmapped;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSource.Numeric;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.profile.Profilers;
import org.elasticsearch.search.profile.aggregation.ProfilingAggregator;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

public class AbstractRangeAggregatorFactory<R extends Range> extends ValuesSourceAggregatorFactory<ValuesSource.Numeric> {

    private final InternalRange.Factory<?, ?> rangeFactory;
    private final R[] ranges;
    private final boolean keyed;

    AbstractRangeAggregatorFactory(String name,
                                   ValuesSourceConfig<Numeric> config,
                                   R[] ranges,
                                   boolean keyed,
                                   InternalRange.Factory<?, ?> rangeFactory,
                                   QueryShardContext queryShardContext,
                                   AggregatorFactory parent,
                                   AggregatorFactories.Builder subFactoriesBuilder,
                                   Map<String, Object> metaData) throws IOException {
        super(name, config, queryShardContext, parent, subFactoriesBuilder, metaData);
        this.ranges = ranges;
        this.keyed = keyed;
        this.rangeFactory = rangeFactory;
    }

    @Override
    protected Aggregator createUnmapped(SearchContext searchContext,
                                        Aggregator parent,
                                        List<PipelineAggregator> pipelineAggregators,
                                        Map<String, Object> metaData) throws IOException {
        return new Unmapped<>(name, ranges, keyed, config.format(), searchContext, parent, rangeFactory, pipelineAggregators, metaData);
    }

    @Override
    protected Aggregator doCreateInternal(Numeric valuesSource,
                                          SearchContext searchContext,
                                          Aggregator parent,
                                          boolean collectsFromSingleBucket,
                                          List<PipelineAggregator> pipelineAggregators,
                                          Map<String, Object> metaData) throws IOException {

        // If we don't have a parent, the range agg can potentially optimize by using the BKD tree. But BKD
        // traversal is per-range, which means that docs are potentially called out-of-order across multiple
        // ranges. To prevent this from causing problems, we create a special AggregatorFactories that
        // wraps all the sub-aggs with a MultiBucketAggregatorWrapper. This effectively creates a new agg
        // sub-tree for each range and prevents out-of-order problems
        BiFunction<Number, Boolean, byte[]> pointEncoder = configurePointEncoder(searchContext, parent, config);
        AggregatorFactories wrappedFactories = factories;
        if (pointEncoder != null) {
            wrappedFactories = wrapSubAggsAsMultiBucket(factories);
        }

        return new RangeAggregator(name, wrappedFactories, valuesSource, config, rangeFactory, ranges, keyed, searchContext, parent,
            pipelineAggregators, metaData, pointEncoder);
    }

    /**
     * Returns a converter for point values if BKD optimization is applicable to
     * the context or <code>null</code> otherwise.  Optimization criteria is:
     * - Match_all query
     * - no parent agg
     * - no script
     * - no missing value
     * - has indexed points
     *
     * @param context The {@link SearchContext} of the aggregation.
     * @param parent The parent aggregator.
     * @param config The config for the values source metric.
     */
    private BiFunction<Number, Boolean, byte[]> configurePointEncoder(SearchContext context, Aggregator parent,
                                                                      ValuesSourceConfig<?> config) {
        if (context.query() != null &&
            context.query().getClass() != MatchAllDocsQuery.class) {
            return null;
        }
        if (parent != null) {
            return null;
        }
        if (config.fieldContext() != null && config.script() == null && config.missing() == null) {
            MappedFieldType fieldType = config.fieldContext().fieldType();
            if (fieldType == null || fieldType.indexOptions() == IndexOptions.NONE) {
                return null;
            }
            if (fieldType instanceof NumberFieldMapper.NumberFieldType) {
                return ((NumberFieldMapper.NumberFieldType) fieldType)::encodePoint;
            } else if (fieldType.getClass() == DateFieldMapper.DateFieldType.class) {
                return NumberFieldMapper.NumberType.LONG::encodePoint;
            }
        }
        return null;
    }

    /**
     * Creates a new{@link AggregatorFactories} object so that sub-aggs are automatically
     * wrapped with a {@link org.elasticsearch.search.aggregations.AggregatorFactory.MultiBucketAggregatorWrapper}.
     * This allows sub-aggs to execute in their own isolated sub tree
     */
    private static AggregatorFactories wrapSubAggsAsMultiBucket(AggregatorFactories factories) {
        return new AggregatorFactories(factories.getFactories(), factories.getPipelineAggregatorFactories()) {
            @Override
            public Aggregator[] createSubAggregators(SearchContext searchContext, Aggregator parent) throws IOException {
                Aggregator[] aggregators = new Aggregator[countAggregators()];
                for (int i = 0; i < this.factories.length; ++i) {
                    Aggregator factory = asMultiBucketAggregator(factories[i], searchContext, parent);
                    Profilers profilers = factory.context().getProfilers();
                    if (profilers != null) {
                        factory = new ProfilingAggregator(factory, profilers.getAggregationProfiler());
                    }
                    aggregators[i] = factory;
                }
                return aggregators;
            }
        };
    }

}

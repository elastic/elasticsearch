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

package org.elasticsearch.search.aggregations.bucket.histogram;

import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.rounding.Rounding;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.InternalAggregation.Type;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSource.Numeric;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;

public abstract class AbstractHistogramAggregatorFactory<AF extends AbstractHistogramAggregatorFactory<AF>>
        extends ValuesSourceAggregatorFactory<ValuesSource.Numeric, AF> {

    protected final long interval;
    protected final long offset;
    protected final InternalOrder order;
    protected final boolean keyed;
    protected final long minDocCount;
    protected final ExtendedBounds extendedBounds;
    private final InternalHistogram.Factory<?> histogramFactory;

    public AbstractHistogramAggregatorFactory(String name, Type type, ValuesSourceConfig<Numeric> config, long interval, long offset,
            InternalOrder order, boolean keyed, long minDocCount, ExtendedBounds extendedBounds,
            InternalHistogram.Factory<?> histogramFactory, AggregationContext context, AggregatorFactory<?> parent,
            AggregatorFactories.Builder subFactoriesBuilder, Map<String, Object> metaData) throws IOException {
        super(name, type, config, context, parent, subFactoriesBuilder, metaData);
        this.interval = interval;
        this.offset = offset;
        this.order = order;
        this.keyed = keyed;
        this.minDocCount = minDocCount;
        this.extendedBounds = extendedBounds;
        this.histogramFactory = histogramFactory;
    }

    public long minDocCount() {
        return minDocCount;
    }

    @Override
    protected Aggregator createUnmapped(Aggregator parent, List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData)
            throws IOException {
        Rounding rounding = createRounding();
        return new HistogramAggregator(name, factories, rounding, order, keyed, minDocCount, extendedBounds, null, config.format(),
                histogramFactory, context, parent, pipelineAggregators, metaData);
    }

    protected Rounding createRounding() {
        if (interval < 1) {
            throw new ParsingException(null, "[interval] must be 1 or greater for histogram aggregation [" + name() + "]: " + interval);
        }

        Rounding rounding = new Rounding.Interval(interval);
        if (offset != 0) {
            rounding = new Rounding.OffsetRounding(rounding, offset);
        }
        return rounding;
    }

    @Override
    protected Aggregator doCreateInternal(ValuesSource.Numeric valuesSource, Aggregator parent, boolean collectsFromSingleBucket,
            List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) throws IOException {
        if (collectsFromSingleBucket == false) {
            return asMultiBucketAggregator(this, context, parent);
        }
        Rounding rounding = createRounding();
        // we need to round the bounds given by the user and we have to do it
        // for every aggregator we create
        // as the rounding is not necessarily an idempotent operation.
        // todo we need to think of a better structure to the factory/agtor
        // code so we won't need to do that
        ExtendedBounds roundedBounds = null;
        if (extendedBounds != null) {
            // we need to process & validate here using the parser
            extendedBounds.processAndValidate(name, context.searchContext(), config.format());
            roundedBounds = extendedBounds.round(rounding);
        }
        return new HistogramAggregator(name, factories, rounding, order, keyed, minDocCount, roundedBounds, valuesSource,
                config.format(), histogramFactory, context, parent, pipelineAggregators, metaData);
    }

}

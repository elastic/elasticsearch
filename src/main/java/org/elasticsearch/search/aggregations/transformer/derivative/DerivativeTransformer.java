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

package org.elasticsearch.search.aggregations.transformer.derivative;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.search.aggregations.AggregationInitializationException;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.NumericMetricsAggregatorFactory;
import org.elasticsearch.search.aggregations.bucket.histogram.HistogramAggregator;
import org.elasticsearch.search.aggregations.bucket.histogram.InternalHistogram;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.format.ValueFormatter;
import org.elasticsearch.search.aggregations.transformer.Transformer;
import org.elasticsearch.search.aggregations.transformer.derivative.Derivative.GapPolicy;

import java.util.Map;

public class DerivativeTransformer extends Transformer {

    private boolean keyed;
    private @Nullable ValueFormatter formatter;
    private GapPolicy gapPolicy;

    protected DerivativeTransformer(String name, boolean keyed, @Nullable ValueFormatter formatter, GapPolicy gapPolicy, AggregatorFactories factories,
            AggregationContext aggregationContext, Aggregator parent,
            Map<String, Object> metaData) {
        super(name, factories, aggregationContext, parent, metaData);
        this.keyed = keyed;
        this.formatter = formatter;
        this.gapPolicy = gapPolicy;
    }

    @Override
    protected InternalAggregation buildAggregation(String name, int bucketDocCount, InternalAggregations bucketAggregations) {
        return new InternalDerivative<InternalHistogram.Bucket>(name, keyed, formatter, gapPolicy, bucketAggregations, getMetaData());
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalDerivative<InternalHistogram.Bucket>(name, keyed, formatter, gapPolicy, InternalAggregations.EMPTY,
                getMetaData());
    }

    public static class Factory extends AggregatorFactory {

        private boolean keyed;
        private ValueFormatter formatter;
        private GapPolicy gapPolicy;

        public Factory(String name, boolean keyed, @Nullable ValueFormatter formatter, GapPolicy gapPolicy) {
            super(name, InternalDerivative.TYPE.name());
            this.keyed = keyed;
            this.formatter = formatter;
            this.gapPolicy = gapPolicy;
        }

        @Override
        protected Aggregator createInternal(AggregationContext context, Aggregator parent, long expectedBucketsCount,
                Map<String, Object> metaData) {
            return new DerivativeTransformer(name, keyed, formatter, gapPolicy, factories, context, parent, metaData);
        }

        @Override
        public AggregatorFactory subFactories(AggregatorFactories subFactories) {
            AggregatorFactory[] factories = subFactories.factories();
            if (factories[0] instanceof HistogramAggregator.Factory) {
                ((HistogramAggregator.Factory) factories[0]).minDocCount(0);
            }
            return super.subFactories(subFactories);
        }

        @Override
        public void doValidate() {
            AggregatorFactory[] subFactories = factories.factories();
            if (subFactories.length != 1) {
                throw new AggregationInitializationException("Derivative aggregations just have a single sub-aggregation. Found ["
                        + subFactories.length + "] in [" + name + "]");
            } else {
                if (subFactories[0] instanceof HistogramAggregator.Factory) {
                    AggregatorFactory aggregator = subFactories[0];
                    AggregatorFactory[] subAggregatorFactories = aggregator.subFactories().factories();
                    for (AggregatorFactory subAggregator : subAggregatorFactories) {
                        if (!(subAggregator instanceof NumericMetricsAggregatorFactory)) {
                            throw new AggregationInitializationException("Sub-aggregation of [" + aggregator.name()
                                    + "] must be numeric metric aggregations when used in a derivative.");
                        }
                    }
                } else if (!(subFactories[0] instanceof DerivativeTransformer.Factory)) {
                    throw new AggregationInitializationException("Sub-aggregation of [" + name
                            + "] must be one of [histogram, date_histogram, derivative]");
                }
            }
        }

    }
}

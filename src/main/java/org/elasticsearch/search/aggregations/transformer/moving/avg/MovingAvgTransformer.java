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

package org.elasticsearch.search.aggregations.transformer.moving.avg;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.search.aggregations.*;
import org.elasticsearch.search.aggregations.bucket.histogram.HistogramAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.format.ValueFormatter;
import org.elasticsearch.search.aggregations.transformer.DiscontinuousHistogram;
import org.elasticsearch.search.aggregations.transformer.Transformer;
import org.elasticsearch.search.aggregations.transformer.models.MovingAvgModel;

import java.io.IOException;
import java.util.Map;

public class MovingAvgTransformer extends Transformer {

    private boolean keyed;
    private @Nullable ValueFormatter formatter;
    private DiscontinuousHistogram.GapPolicy gapPolicy;
    private int window;
    private MovingAvgModel.Weighting weight;

    protected MovingAvgTransformer(String name, boolean keyed, @Nullable ValueFormatter formatter, DiscontinuousHistogram.GapPolicy gapPolicy, int window,
                                   MovingAvgModel.Weighting weight,
                                   AggregatorFactories factories,
                                   AggregationContext aggregationContext, Aggregator parent,
                                   Map<String, Object> metaData)  throws IOException {
        super(name, factories, aggregationContext, parent, metaData);
        this.keyed = keyed;
        this.formatter = formatter;
        this.gapPolicy = gapPolicy;
        this.window = window;
        this.weight = weight;
    }

    @Override
    protected InternalAggregation buildAggregation(String name, int bucketDocCount, InternalAggregations bucketAggregations) {
        return new InternalMovingAvg<>(name, gapPolicy, window, weight, bucketAggregations, metaData());
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalMovingAvg<>(name, gapPolicy, window, weight, InternalAggregations.EMPTY, metaData());
    }

    public static class Factory extends AggregatorFactory {

        private boolean keyed;
        private ValueFormatter formatter;
        private DiscontinuousHistogram.GapPolicy gapPolicy;
        private int window;
        private MovingAvgModel.Weighting weight;

        public Factory(String name, boolean keyed, @Nullable ValueFormatter formatter, DiscontinuousHistogram.GapPolicy gapPolicy, int window, MovingAvgModel.Weighting weight) {
            super(name, InternalMovingAvg.TYPE.name());
            this.keyed = keyed;
            this.formatter = formatter;
            this.gapPolicy = gapPolicy;
            this.window = window;
            this.weight = weight;
        }

        @Override
        protected Aggregator createInternal(AggregationContext context, Aggregator parent, boolean collectsFromSingleBucket,
                                            Map<String, Object> metaData) throws IOException {
            return new MovingAvgTransformer(name, keyed, formatter, gapPolicy, window, weight, factories, context, parent, metaData);
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
                } else if (!(subFactories[0] instanceof MovingAvgTransformer.Factory)) {
                    throw new AggregationInitializationException("Sub-aggregation of [" + name
                            + "] must be one of [histogram, date_histogram, derivative]");
                }
            }
        }

    }
}

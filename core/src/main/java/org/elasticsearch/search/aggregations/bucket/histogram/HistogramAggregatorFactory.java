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

import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.InternalAggregation.Type;
import org.elasticsearch.search.aggregations.support.ValuesSource.Numeric;

import java.io.IOException;
import java.util.Map;

import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;

public class HistogramAggregatorFactory extends AbstractHistogramAggregatorFactory<HistogramAggregatorFactory> {

    public HistogramAggregatorFactory(String name, Type type, ValuesSourceConfig<Numeric> config, long interval, long offset,
            InternalOrder order, boolean keyed, long minDocCount, ExtendedBounds extendedBounds, AggregationContext context,
            AggregatorFactory<?> parent, AggregatorFactories.Builder subFactoriesBuilder, Map<String, Object> metaData) throws IOException {
        super(name, type, config, interval, offset, order, keyed, minDocCount, extendedBounds, InternalHistogram.HISTOGRAM_FACTORY, context,
                parent, subFactoriesBuilder, metaData);
    }

}

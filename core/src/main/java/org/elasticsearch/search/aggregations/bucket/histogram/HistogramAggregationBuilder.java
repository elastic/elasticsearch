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

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.search.aggregations.AggregatorFactories.Builder;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSource.Numeric;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;

import java.io.IOException;

public class HistogramAggregationBuilder extends AbstractHistogramBuilder<HistogramAggregationBuilder> {
    public static final String NAME = InternalHistogram.TYPE.name();
    public static final ParseField AGGREGATION_NAME_FIELD = new ParseField(NAME);

    public HistogramAggregationBuilder(String name) {
        super(name, InternalHistogram.HISTOGRAM_FACTORY);
    }

    /**
     * Read from a stream.
     */
    public HistogramAggregationBuilder(StreamInput in) throws IOException {
        super(in, InternalHistogram.HISTOGRAM_FACTORY);
    }

    @Override
    protected HistogramAggregatorFactory innerBuild(AggregationContext context, ValuesSourceConfig<Numeric> config,
            AggregatorFactory<?> parent, Builder subFactoriesBuilder) throws IOException {
        return new HistogramAggregatorFactory(name, type, config, interval, offset, order, keyed, minDocCount, extendedBounds, context,
                parent, subFactoriesBuilder, metaData);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }
}

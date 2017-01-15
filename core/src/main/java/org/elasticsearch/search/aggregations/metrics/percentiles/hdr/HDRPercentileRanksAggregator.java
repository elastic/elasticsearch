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
package org.elasticsearch.search.aggregations.metrics.percentiles.hdr;

import org.HdrHistogram.DoubleHistogram;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.ValuesSource.Numeric;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class HDRPercentileRanksAggregator extends AbstractHDRPercentilesAggregator {

    public HDRPercentileRanksAggregator(String name, Numeric valuesSource, SearchContext context, Aggregator parent,
            double[] percents, int numberOfSignificantValueDigits, boolean keyed, DocValueFormat format,
            List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) throws IOException {
        super(name, valuesSource, context, parent, percents, numberOfSignificantValueDigits, keyed, format, pipelineAggregators,
                metaData);
    }

    @Override
    public InternalAggregation buildAggregation(long owningBucketOrdinal) {
        DoubleHistogram state = getState(owningBucketOrdinal);
        if (state == null) {
            return buildEmptyAggregation();
        } else {
            return new InternalHDRPercentileRanks(name, keys, state, keyed, format, pipelineAggregators(), metaData());
        }
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        DoubleHistogram state;
        state = new DoubleHistogram(numberOfSignificantValueDigits);
        state.setAutoResize(true);
        return new InternalHDRPercentileRanks(name, keys, state,
                keyed, format, pipelineAggregators(), metaData());
    }

    @Override
    public double metric(String name, long bucketOrd) {
        DoubleHistogram state = getState(bucketOrd);
        if (state == null) {
            return Double.NaN;
        } else {
            return InternalHDRPercentileRanks.percentileRank(state, Double.valueOf(name));
        }
    }
}

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
package org.elasticsearch.search.aggregations.metrics.stats.multifield;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.ObjectArray;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.metrics.MetricsAggregator;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Metric Aggregation for computing the pearson product correlation coefficient between multiple fields
 **/
public class MultiFieldStatsAggregator extends MetricsAggregator {
    /** Multiple ValuesSource with field names */
    final Map<String, ValuesSource.Numeric> valuesSources;

    /** array of descriptive stats, per shard, needed to compute the correlation */
    ObjectArray<MultiFieldStatsResults> correlationStats;

    public MultiFieldStatsAggregator(String name, Map<String, ValuesSource.Numeric> valuesSources, AggregationContext context,
                                     Aggregator parent, List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) throws IOException {
        super(name, context, parent, pipelineAggregators, metaData);
        this.valuesSources = valuesSources;
        if (valuesSources != null && !valuesSources.isEmpty()) {
            correlationStats = context.bigArrays().newObjectArray(1);
        }
    }

    @Override
    public boolean needsScores() {
        boolean needsScores = false;
        if (valuesSources != null) {
            for (Map.Entry<String, ValuesSource.Numeric> valueSource : valuesSources.entrySet()) {
                needsScores |= valueSource.getValue().needsScores();
            }
        }
        return needsScores;
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx,
                                                final LeafBucketCollector sub) throws IOException {
        if (valuesSources == null || valuesSources.isEmpty()) {
            return LeafBucketCollector.NO_OP_COLLECTOR;
        }
        final BigArrays bigArrays = context.bigArrays();
        final HashMap<String, SortedNumericDoubleValues> values = new HashMap<>(valuesSources.size());
        for (Map.Entry<String, ValuesSource.Numeric> valuesSource : valuesSources.entrySet()) {
            values.put(valuesSource.getKey(), valuesSource.getValue().doubleValues(ctx));
        }

        return new LeafBucketCollectorBase(sub, values) {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                // get fields
                Map<String, Double> fields = getFields(doc);
                if (fields != null) {
                    correlationStats = bigArrays.grow(correlationStats, bucket + 1);
                    MultiFieldStatsResults correlationStat = correlationStats.get(bucket);
                    if (correlationStat == null) {
                        correlationStat = new MultiFieldStatsResults();
                    }
                    // add document fields to correlation stats
                    correlationStat.add(fields);
                    correlationStats.set(bucket, correlationStat);
                }
            }

            /**
             * return a map of field names and data
             */
            private Map<String, Double> getFields(int doc) {
                // get fieldNames to use as hash keys
                ArrayList<String> fieldNames = new ArrayList<>(values.keySet());
                HashMap<String, Double> fields = new HashMap<>(fieldNames.size());

                // loop over fields
                for (String fieldName : fieldNames) {
                    final SortedNumericDoubleValues doubleValues = values.get(fieldName);
                    doubleValues.setDocument(doc);
                    final int valuesCount = doubleValues.count();
                    // if document contains an empty field we omit the doc from the correlation
                    if (valuesCount <= 0) {
                        return null;
                    }
                    // get the field value (multi-value is the average of all the values)
                    double fieldValue = 0;
                    for (int i = 0; i < valuesCount; ++i) {
                        fieldValue += doubleValues.valueAt(i);
                    }
                    fields.put(fieldName, fieldValue / valuesCount);
                }
                return fields;
            }
        };
    }

    @Override
    public InternalAggregation buildAggregation(long bucket) {
        if (valuesSources == null || bucket >= correlationStats.size()) {
            return buildEmptyAggregation();
        }
        return new InternalMultiFieldStats(name, correlationStats.size(), correlationStats.get(bucket),
            pipelineAggregators(), metaData());
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalMultiFieldStats(name, 0, null, pipelineAggregators(), metaData());
    }

    @Override
    public void doClose() {
        Releasables.close(correlationStats);
    }
}

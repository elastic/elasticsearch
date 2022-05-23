/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.timeseries.aggregation.bucketfunction;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.timeseries.aggregation.TSIDValue;
import org.elasticsearch.search.aggregations.timeseries.aggregation.internal.TSIDInternalAggregation;

import java.util.HashMap;
import java.util.Map;

/**
 * The function is used to aggregator time series lines in the coordinate reduce phase.
 * The _tsid may be exist in many indices, when the bucket ranges will overflow the range of the index,
 * it may be exist
 * e.g a index settings and query config is:<ul>
 * <li>time_series.start_time = 10
 * <li>time_series.end_time = 20
 * <li>interval = 2
 * </ul>
 * When the bucket range is 11-13, the bucket must only in the index.
 * But if the bucket range is 9-11, the bucket may be include other index, so the aggregator function
 * can't compute in the datanode. the tsid bucket function gather all _tsid and the value, and aggregator
 * the result in the coordinate reduce phase.
 */
@SuppressWarnings({ "unchecked", "rawtypes" })
public class TSIDBucketFunction implements AggregatorBucketFunction<TSIDValue> {
    private Map<Long, Map<BytesRef, InternalAggregation>> values = new HashMap<>();
    private final AggregatorBucketFunction aggregatorBucketFunction;

    public TSIDBucketFunction(AggregatorBucketFunction aggregatorBucketFunction) {
        this.aggregatorBucketFunction = aggregatorBucketFunction;
    }

    @Override
    public String name() {
        return "tsid";
    }

    @Override
    public void collect(TSIDValue tsidValue, long bucket) {
        if (tsidValue.detailed) {
            Map<BytesRef, InternalAggregation> tsidValues = values.get(bucket);
            if (tsidValues == null) {
                tsidValues = new HashMap<>();
                values.put(bucket, tsidValues);
            }
            tsidValues.put(tsidValue.tsid, (InternalAggregation) tsidValue.value);
        } else {
            aggregatorBucketFunction.collect(tsidValue.value, bucket);
        }
    }

    @Override
    public void close() {
        aggregatorBucketFunction.close();
    }

    @Override
    public InternalAggregation getAggregation(long bucket, DocValueFormat formatter, Map<String, Object> metadata) {
        if (values.containsKey(bucket)) {
            return new TSIDInternalAggregation(name(), values.get(bucket), aggregatorBucketFunction.name(), formatter, metadata);
        } else {
            return aggregatorBucketFunction.getAggregation(bucket, formatter, metadata);
        }
    }

}

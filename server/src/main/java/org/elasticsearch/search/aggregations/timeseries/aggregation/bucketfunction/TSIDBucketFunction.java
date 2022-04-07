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
import org.elasticsearch.search.aggregations.timeseries.aggregation.bucketfunction.TSIDBucketFunction.TSIDValue;
import org.elasticsearch.search.aggregations.timeseries.aggregation.internal.TSIDInternalAggregation;

import java.util.HashMap;
import java.util.Map;

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

    public static class TSIDValue {
        public TSIDValue(BytesRef tsid, Object value, boolean detailed) {
            this.tsid = tsid;
            this.value = value;
            this.detailed = detailed;
        }

        BytesRef tsid;
        Object value;
        boolean detailed;
    }
}

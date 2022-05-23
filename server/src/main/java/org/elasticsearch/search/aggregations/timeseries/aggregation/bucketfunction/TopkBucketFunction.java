/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.timeseries.aggregation.bucketfunction;

import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.util.PriorityQueue;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.timeseries.aggregation.TSIDValue;

public class TopkBucketFunction implements AggregatorBucketFunction<TSIDValue<Double>>{

    Map<Long, PriorityQueue<TSIDValue<Double>>> values;
    private int topkSize;

    public TopkBucketFunction(int size) {
        values = new HashMap<>();
        this.topkSize = size;
    }

    @Override
    public String name() {
        return "topk";
    }

    @Override
    public void collect(TSIDValue<Double> number, long bucket) {
        PriorityQueue<TSIDValue<Double>> queue = values.get(bucket);
        if (queue == null) {
            queue = new PriorityQueue<>(topkSize) {
                @Override
                protected boolean lessThan(TSIDValue<Double> a, TSIDValue<Double> b) {
                    return a.value < b.value;
                }
            };
            values.put(bucket, queue);
        }

        queue.add(number);
    }

    @Override
    public InternalAggregation getAggregation(long bucket,
        DocValueFormat formatter, Map<String, Object> metadata) {
        return null;
    }

    @Override
    public void close() {
        values = null;
    }
}

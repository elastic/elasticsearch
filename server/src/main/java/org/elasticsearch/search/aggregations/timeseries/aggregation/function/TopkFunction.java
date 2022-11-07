/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.timeseries.aggregation.function;

import org.apache.lucene.util.PriorityQueue;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.timeseries.aggregation.TSIDValue;
import org.elasticsearch.search.aggregations.timeseries.aggregation.internal.TimeSeriesTopk;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TopkFunction implements AggregatorFunction<TSIDValue<Double>, List<TSIDValue<Double>>> {
    private final PriorityQueue<TSIDValue<Double>> queue;
    private final int topkSize;
    private final boolean isTop;

    public TopkFunction(int size, boolean isTop) {
        queue = getTopkQueue(size, isTop);
        this.isTop = isTop;
        this.topkSize = size;
    }

    @Override
    public void collect(TSIDValue<Double> value) {
        queue.insertWithOverflow(value);
    }

    @Override
    public List<TSIDValue<Double>> get() {
        List<TSIDValue<Double>> values = new ArrayList<>(queue.size());
        for (int b = queue.size() - 1; b >= 0; --b) {
            values.add(queue.pop());
        }
        return values;
    }

    @Override
    public InternalAggregation getAggregation(DocValueFormat formatter, Map<String, Object> metadata) {
        return new TimeSeriesTopk(TimeSeriesTopk.NAME, get(), topkSize, isTop, formatter, metadata);
    }

    public static PriorityQueue<TSIDValue<Double>> getTopkQueue(int size, boolean isTop) {
        return new PriorityQueue<>(size) {
            @Override
            protected boolean lessThan(TSIDValue<Double> a, TSIDValue<Double> b) {
                if (isTop) {
                    return a.value < b.value;
                } else {
                    return a.value > b.value;
                }
            }
        };
    }

}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.timeseries;

import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.iterable.Iterables;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The merged histogram and the list of failed shards.
 */
public final class TsidPrefixHistogramResponse extends BroadcastResponse {
    private final TsidPrefixHistogram histogram;
    private final Map<String, Exception> clusterFailures;

    TsidPrefixHistogramResponse(
        int totalShards,
        int successfulShards,
        int failedShards,
        List<DefaultShardOperationFailedException> shardFailures,
        TsidPrefixHistogram histogram,
        Map<String, Exception> clusterFailures
    ) {
        super(totalShards, successfulShards, failedShards, shardFailures);
        this.histogram = histogram;
        this.clusterFailures = clusterFailures;
    }

    TsidPrefixHistogramResponse(StreamInput in) throws IOException {
        super(in);
        this.histogram = new TsidPrefixHistogram(in);
        this.clusterFailures = Map.of();
    }

    static TsidPrefixHistogramResponse clusterFailure(String clusterAlias, Exception e) {
        return new TsidPrefixHistogramResponse(0, 0, 0, List.of(), TsidPrefixHistogram.EMPTY, Map.of(clusterAlias, e));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        histogram.writeTo(out);
    }

    public TsidPrefixHistogram histogram() {
        return histogram;
    }

    public Map<String, Exception> clusterFailures() {
        return clusterFailures;
    }

    static TsidPrefixHistogramResponse mergeResponses(Collection<TsidPrefixHistogramResponse> responses) {
        if (responses.size() == 1) {
            return Iterables.get(responses, 0);
        }
        int totalShards = 0;
        int successfulShards = 0;
        int failedShards = 0;
        List<DefaultShardOperationFailedException> shardFailures = new ArrayList<>();
        Map<String, Exception> clusterFailures = new HashMap<>();
        TsidPrefixHistogram histogram = TsidPrefixHistogram.EMPTY;
        for (TsidPrefixHistogramResponse response : responses) {
            totalShards += response.getTotalShards();
            successfulShards += response.getSuccessfulShards();
            failedShards += response.getFailedShards();
            shardFailures.addAll(response.getShardFailures());
            clusterFailures.putAll(response.clusterFailures());
            histogram = TsidPrefixHistogram.merge(histogram, response.histogram());
        }
        return new TsidPrefixHistogramResponse(
            totalShards,
            successfulShards,
            failedShards,
            shardFailures,
            histogram.capped(TsidPrefixHistogram.MAX_CELLS),
            clusterFailures
        );
    }
}

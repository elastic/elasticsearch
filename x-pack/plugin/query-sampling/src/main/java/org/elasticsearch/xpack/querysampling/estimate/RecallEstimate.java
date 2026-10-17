/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling.estimate;

import org.elasticsearch.core.Nullable;

import java.util.List;

/**
 * What the sampled queries say about the recall of the search.
 *
 * @param trafficWeightedRecall  recall as users experience it (a query counts as often as it is searched), or {@code null}
 *                               if no query could be used
 * @param trafficEffectiveSize   how many equally weighted queries the estimate is worth; falls when a few carry most weight
 * @param uniqueQueryRecall      recall averaged over distinct queries, each counting once, or {@code null} if none could be used
 * @param byCluster              low-recall regions are hidden in the overall average when most queries are elsewhere
 * @param events                 the recall of the traffic from the uniform sample of arrivals, without the sampler's weights
 */
public record RecallEstimate(
    int records,
    int recordsWithGroundTruth,
    @Nullable Double trafficWeightedRecall,
    double trafficEffectiveSize,
    @Nullable Double uniqueQueryRecall,
    double uniqueQueryEffectiveSize,
    List<GroupEstimate> byHardness,
    List<GroupEstimate> byCluster,
    List<GroupEstimate> bySelectivity,
    EventEstimate events
) {

    /**
     * The estimate from events; each is a search, so it is weighted only by the probability of having been kept.
     */
    public record EventEstimate(int records, int recordsWithGroundTruth, @Nullable Double recall, double effectiveSize) {}

    /**
     * The estimates for one stratum, in the same terms as those of all the queries.
     */
    public record GroupEstimate(
        String key,
        int recordsWithGroundTruth,
        @Nullable Double trafficWeightedRecall,
        double trafficEffectiveSize,
        @Nullable Double uniqueQueryRecall,
        double uniqueQueryEffectiveSize
    ) {}
}

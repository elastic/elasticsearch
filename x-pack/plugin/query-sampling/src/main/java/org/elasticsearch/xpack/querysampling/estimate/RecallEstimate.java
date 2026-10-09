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
 * @param records                   sampled queries that were looked at
 * @param recordsWithGroundTruth    of those, queries that could be used because their ground truth is known
 * @param trafficWeightedRecall     recall of the search as users experience it: a query counts as often as it is
 *                                  searched. {@code null} if no query could be used
 * @param trafficEffectiveSize      how many equally weighted queries this estimate is worth. It is the number that
 *                                  tells how much to trust the estimate: it falls when a few queries carry most of the
 *                                  weight
 * @param uniqueQueryRecall         recall averaged over distinct queries, each counting once however often it is
 *                                  searched. {@code null} if no query could be used
 * @param uniqueQueryEffectiveSize  the same measure for the estimate over distinct queries
 * @param byHardness                the same estimates for the queries of each hardness, those that have one
 * @param byCluster                 the same estimates for the queries of each cluster of the vector space, those that
 *                                  have one. A region where the recall is low is not seen in the averages of all the
 *                                  queries when most of them are somewhere else
 * @param events                    what the uniform sample of arrivals says: the recall of the traffic, estimated without
 *                                  the weights of the sampler
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
    EventEstimate events
) {

    /**
     * The estimate from the arrivals that were kept as events. Each is a search, so the average is that of the traffic,
     * and it is weighted only by the probability of having been kept.
     *
     * @param records                 events that were looked at
     * @param recordsWithGroundTruth  of those, events that could be used because their ground truth is known
     * @param recall                  recall of the search as users experience it, {@code null} if no event could be used
     * @param effectiveSize           how many equally weighted events the estimate is worth
     */
    public record EventEstimate(int records, int recordsWithGroundTruth, @Nullable Double recall, double effectiveSize) {}

    /**
     * The estimates for the queries of one stratum, in the same terms as those of all the queries.
     *
     * @param key                      the stratum: the hardness, or the space of the vectors and the number of the cluster
     * @param recordsWithGroundTruth   queries of the stratum that could be used
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

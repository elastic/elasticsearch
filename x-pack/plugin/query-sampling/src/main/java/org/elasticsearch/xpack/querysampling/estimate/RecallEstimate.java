/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling.estimate;

import org.elasticsearch.core.Nullable;

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
 */
public record RecallEstimate(
    int records,
    int recordsWithGroundTruth,
    @Nullable Double trafficWeightedRecall,
    double trafficEffectiveSize,
    @Nullable Double uniqueQueryRecall,
    double uniqueQueryEffectiveSize
) {}

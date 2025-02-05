/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation.allocator;

import java.util.List;

/**
 * Holds combined {@link BalancingRoundSummary} results. Essentially holds a list of the balancing events and the summed up changes
 * across all those events: what allocation work was done across some period of time.
 * TODO: WIP ES-10341
 *
 * Note that each balancing round summary is the difference between, at the time, latest desired balance and the previous desired balance.
 * Each summary represents a step towards the next desired balance, which is based on presuming the previous desired balance is reached. So
 * combining them is roughly the difference between the first summary's previous desired balance and the last summary's latest desired
 * balance.
 *
 * @param numberOfBalancingRounds How many balancing round summaries are combined in this report.
 * @param numberOfShardMoves The sum of shard moves for each balancing round being combined into a single summary.
 */
public record CombinedBalancingRoundSummary(int numberOfBalancingRounds, long numberOfShardMoves) {

    public static final CombinedBalancingRoundSummary EMPTY_RESULTS = new CombinedBalancingRoundSummary(0, 0);

    public static CombinedBalancingRoundSummary combine(List<BalancingRoundSummary> summaries) {
        if (summaries.isEmpty()) {
            return EMPTY_RESULTS;
        }

        int numSummaries = 0;
        long numberOfShardMoves = 0;
        for (BalancingRoundSummary summary : summaries) {
            ++numSummaries;
            numberOfShardMoves += summary.numberOfShardsToMove();
        }
        return new CombinedBalancingRoundSummary(numSummaries, numberOfShardMoves);
    }

}

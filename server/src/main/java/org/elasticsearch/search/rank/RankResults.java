/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.rank;

import java.util.List;

public class RankResults {

    public static class RankResult {
        public final int doc;
        public final int shard;
        public float rank;
        public final int[] positions;

        public RankResult(int doc, int shard, float rank, int size) {
            this.doc = doc;
            this.shard = shard;
            this.rank = rank;
            this.positions = new int[size];
        }
    }

    private final List<RankResult> rankResults;

    public RankResults(List<RankResult> rankResults) {
        this.rankResults = rankResults;
    }
}

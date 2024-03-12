/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.rank;

import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.elasticsearch.search.rank.RankShardContext;
import org.elasticsearch.search.rank.RankShardResult;

import java.util.List;

public class TextSimilarityRankShardContext extends RankShardContext {

    public TextSimilarityRankShardContext(List<Query> queries, int from, int windowSize) {
        super(queries, from, windowSize);
    }

    @Override
    public RankShardResult combine(List<TopDocs> rankResults) {
        return null;
    }

}

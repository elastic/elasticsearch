/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.rank.script;

import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.elasticsearch.search.rank.RankShardContext;
import org.elasticsearch.search.rank.RankShardResult;

import java.util.List;

public class ScriptRankShardContext extends RankShardContext {

    public ScriptRankShardContext(List<Query> queries, int from, int windowSize) {
        super(queries, from, windowSize);
    }

    @Override
    public RankShardResult combine(List<TopDocs> rankResults) {
        return new ScriptRankShardResult(rankResults);
    }
}

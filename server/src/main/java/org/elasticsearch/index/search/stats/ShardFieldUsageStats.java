/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.search.stats;

import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.elasticsearch.index.shard.SearchOperationListener;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class ShardFieldUsageStats implements SearchOperationListener {

    private final Map<String, InternalFieldStats> perFieldStats = new ConcurrentHashMap<>();

    public void onQuery(Query query) {
        // alternatively, use FieldExtractor (used by security)
        query.visit(new QueryVisitor() {
            @Override
            public boolean acceptField(String field) {
                perFieldStats.computeIfAbsent(field, f -> new InternalFieldStats()).queryCount.incrementAndGet();
                return true;
            }

            @Override
            public void visitLeaf(Query query) {
//                    if (query instanceof TermQuery) {
//                        fieldNames.add(((TermQuery) query).getTerm().field());
//                    }
//                    if (query instanceof MultiTermQuery) {
//                        fieldNames.add(((MultiTermQuery) query).getField());
//                    }
//                    if (query instanceof )
            }
        });
    }

    public void onFieldAggregation(String field) {
        perFieldStats.computeIfAbsent(field, f -> new InternalFieldStats()).aggregationCount.incrementAndGet();
    }

    public void clear() {
        perFieldStats.clear();
    }


    static class InternalFieldStats {
        final AtomicLong queryCount = new AtomicLong();
        final AtomicLong aggregationCount = new AtomicLong();
    }

    public static class FieldStats {
        public FieldStats(long queryCount, long aggregationCount) {
            this.queryCount = queryCount;
            this.aggregationCount = aggregationCount;
        }

        public long getQueryCount() {
            return queryCount;
        }

        private final long queryCount;

        public long getAggregationCount() {
            return aggregationCount;
        }

        private final long aggregationCount;
    }

    public Map<String, FieldStats> getPerFieldStats() {
        final Map<String, FieldStats> stats = new HashMap<>(perFieldStats.size());
        for (Map.Entry<String, InternalFieldStats> entry : perFieldStats.entrySet()) {
            stats.put(entry.getKey(), new FieldStats(entry.getValue().queryCount.get(), entry.getValue().aggregationCount.get()));
        }
        return stats;
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.lucene.search;

import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.Query;

import java.util.Set;

/**
 * Detects {@link MultiTermQuery} and its constant-score rewrite wrappers, all of which build an
 * untracked per-leaf {@code DocIdSet}. The wrappers are package-private in
 * {@code org.apache.lucene.search} and don't extend {@link MultiTermQuery}, so they can only be
 * matched by simple class name here -- the same approach Lucene's own
 * {@code UsageTrackingQueryCachingPolicy#isCostly} takes internally.
 */
public final class CostlyMultiTermQueries {

    /**
     * Simple names of Lucene's package-private constant-score {@link MultiTermQuery} rewrite wrappers, as
     * of {@code lucene-core 10.5.1}. Kept in sync with {@code UsageTrackingQueryCachingPolicy#isCostly}.
     */
    private static final Set<String> CONSTANT_SCORE_WRAPPER_CLASS_NAMES = Set.of(
        "MultiTermQueryConstantScoreWrapper",
        "MultiTermQueryConstantScoreBlendedWrapper"
    );

    private CostlyMultiTermQueries() {}

    /** Whether {@code query} is a {@link MultiTermQuery} or one of its constant-score rewrite wrappers. */
    public static boolean isCostlyMultiTermQuery(Query query) {
        if (query instanceof MultiTermQuery) {
            return true;
        }
        return CONSTANT_SCORE_WRAPPER_CLASS_NAMES.contains(query.getClass().getSimpleName());
    }
}

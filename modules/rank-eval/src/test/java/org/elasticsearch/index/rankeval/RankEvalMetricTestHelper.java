/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.rankeval;

import org.elasticsearch.search.SearchHit;

/**
 * Ref-count helpers for metric unit tests that use pooled {@link SearchHit} instances with
 * {@link RatedSearchHit}'s public constructor ({@link SearchHit#mustIncRef()}).
 */
final class RankEvalMetricTestHelper {

    private RankEvalMetricTestHelper() {}

    /** Undoes one {@link SearchHit#mustIncRef()} per {@link RatedSearchHit} in the evaluation result. */
    static void releaseRatedSearchHitsOnly(EvalQueryQuality quality) {
        for (RatedSearchHit ratedSearchHit : quality.getHitsAndRatings()) {
            ratedSearchHit.getSearchHit().decRef();
        }
    }

    /** Releases the initial refcount from {@code new SearchHit(...)} after all {@link RatedSearchHit} refs are released. */
    static void releaseScratchHits(SearchHit[] scratchHits) {
        for (SearchHit hit : scratchHits) {
            hit.decRef();
        }
    }
}

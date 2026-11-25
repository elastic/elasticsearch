/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.lucene;

import org.elasticsearch.test.ESTestCase;

import java.util.List;

import static org.elasticsearch.lucene.LuceneForks.checkForksAreValid;
import static org.elasticsearch.lucene.LuceneForks.checkForksForExpiry;
import static org.elasticsearch.lucene.LuceneForks.expiring;
import static org.elasticsearch.lucene.LuceneForks.permanent;
import static org.elasticsearch.lucene.LuceneForks.workaround;

public class ServerLuceneForkTests extends ESTestCase {

    private static final String RANGE_FIX_REMOVAL =
        "Delete XSortedSetDocValuesRangeQuery, XSortedSkipperScorerSupplier and XRangeBulkScorer, "
            + "revert callers to SortedSetDocValuesField.newSlowRangeQuery/newSlowExactQuery, and keep the regression tests.";

    public static final List<LuceneForks.Fork> SERVER_FORKS = List.of(
        expiring("org.elasticsearch.lucene.queries.XSortedSetDocValuesRangeQuery", "apache/lucene#16546", 10, 6, 0, RANGE_FIX_REMOVAL),
        expiring("org.elasticsearch.lucene.document.XSortedSkipperScorerSupplier", "apache/lucene#16546", 10, 6, 0, RANGE_FIX_REMOVAL),
        expiring("org.elasticsearch.lucene.document.XRangeBulkScorer", "apache/lucene#16546", 10, 6, 0, RANGE_FIX_REMOVAL),
        workaround(
            "org.elasticsearch.search.vectors.DenseVectorQuery",
            "apache/lucene#16377",
            11,
            0,
            0,
            "Remove the lazy initialisation of bulkScorer in DenseVectorScorer."
        ),
        workaround(
            "org.elasticsearch.index.store.FsDirectoryFactory",
            "apache/lucene#16970",
            10,
            6,
            1,
            "Remove FsDirectoryFactory.HybridDirectory#confineTempStoredFieldsIndex and its test."
        ),
        workaround(
            "org.elasticsearch.index.search.QueryTokenCacheAccountingFilter",
            "apache/lucene#17020",
            11,
            0,
            0,
            "Stop counting the term bytes of CharTermAttributeImpl, whose clone no longer copies them, "
                + "and drop the stale-bytes paragraph from the class Javadoc."
        ),
        workaround(
            "org.elasticsearch.indices.RunEndCachingBitDocIdSet",
            "apache/lucene#16849",
            10,
            6,
            0,
            "Delete RunEndCachingBitDocIdSet and its tests, and the cacheImpl override in IndicesQueryCache."
        ),
        permanent("org.elasticsearch.lucene.comparators.XNumericComparator", "Elasticsearch-specific sort skipping behaviour"),
        permanent("org.elasticsearch.lucene.comparators.XLongComparator", "Elasticsearch-specific sort skipping behaviour"),
        permanent("org.elasticsearch.lucene.comparators.XUpdateableDocIdSetIterator", "Elasticsearch-specific sort skipping behaviour"),
        permanent("org.elasticsearch.common.lucene.search.XMoreLikeThis", "Elasticsearch-specific more-like-this behaviour")
    );

    public void testForks() {
        checkForksAreValid(SERVER_FORKS);
        checkForksForExpiry(SERVER_FORKS);
    }
}

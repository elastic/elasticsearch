/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.lucene;

import org.apache.lucene.util.Version;

import java.util.List;

/**
 * Central registry of Elasticsearch's forks of Lucene classes (the {@code X*} classes).
 *
 * <p>Every fork that exists to carry an upstream bug fix must be registered here with the Lucene
 * release that contains the fix. {@link LuceneForksTests} fails once the Lucene version we build
 * against reaches that release, which is the signal to delete the fork. Forks that are intentional
 * extensions rather than bug fixes are registered with {@link #permanent} so that they can still be
 * found and checked for existence during Lucene upgrades.
 *
 * <p>Note that this list may not be exhaustive - if you find an unregistered fork while looking at
 * other code, please register it here so that we can track it.
 */
public final class LuceneForks {

    /**
     * A forked class.
     *
     * @param className      fully qualified name of the fork; checked to exist, so renames must update the registry
     * @param upstreamIssue  the apache/lucene issue or PR motivating the fork, or a description for permanent forks
     * @param fixedIn        first Lucene release containing the upstream fix, or {@code null} if the fork is permanent
     * @param removalNote    what to do when the fork expires
     */
    public record Fork(String className, String upstreamIssue, Version fixedIn, String removalNote) {

        public boolean isExpired(Version luceneVersion) {
            return fixedIn != null && luceneVersion.onOrAfter(fixedIn);
        }

        public String expiryMessage(Version luceneVersion) {
            return "Lucene "
                + luceneVersion
                + " contains the fix for "
                + upstreamIssue
                + " ("
                + fixedIn
                + "); the fork "
                + className
                + " is no longer needed. "
                + removalNote
                + " Then remove it from LuceneForks.";
        }
    }

    static Fork expiring(String className, String upstreamIssue, int major, int minor, int bugfix, String removalNote) {
        return new Fork(className, upstreamIssue, Version.fromBits(major, minor, bugfix), removalNote);
    }

    static Fork permanent(String className, String reason) {
        return new Fork(className, reason, null, "This fork is intentional and does not expire.");
    }

    private static final String RANGE_FIX_REMOVAL =
        "Delete XSortedSetDocValuesRangeQuery, XSortedSkipperScorerSupplier and XRangeBulkScorer, "
            + "revert callers to SortedSetDocValuesField.newSlowRangeQuery/newSlowExactQuery, and keep the regression tests.";

    public static final List<Fork> ALL = List.of(
        expiring("org.elasticsearch.lucene.queries.XSortedSetDocValuesRangeQuery", "apache/lucene#16546", 10, 6, 0, RANGE_FIX_REMOVAL),
        expiring("org.elasticsearch.lucene.document.XSortedSkipperScorerSupplier", "apache/lucene#16546", 10, 6, 0, RANGE_FIX_REMOVAL),
        expiring("org.elasticsearch.lucene.document.XRangeBulkScorer", "apache/lucene#16546", 10, 6, 0, RANGE_FIX_REMOVAL),
        permanent("org.elasticsearch.lucene.comparators.XNumericComparator", "Elasticsearch-specific sort skipping behaviour"),
        permanent("org.elasticsearch.lucene.comparators.XLongComparator", "Elasticsearch-specific sort skipping behaviour"),
        permanent("org.elasticsearch.lucene.comparators.XUpdateableDocIdSetIterator", "Elasticsearch-specific sort skipping behaviour"),
        permanent("org.elasticsearch.common.lucene.search.XMoreLikeThis", "Elasticsearch-specific more-like-this behaviour")
    );

    private LuceneForks() {}
}

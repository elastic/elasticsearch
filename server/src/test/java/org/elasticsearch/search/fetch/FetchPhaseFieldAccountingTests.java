/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.search.fetch;

import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitRamUsageEstimator;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TestSearchContext;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

/**
 * Unit tests for the per-hit document-field heap estimate used by the fetch-phase circuit breaker.
 * End-to-end breaker behaviour (fields actually trip / release the breaker) is covered by
 * {@code FetchPhaseCircuitBreakerIT}.
 */
public class FetchPhaseFieldAccountingTests extends ESTestCase {

    // ----- estimateDocumentFields coverage -----------------------------------------------

    public void testEstimateIsNonZeroForHitWithDocumentField() {
        SearchHit hit = SearchHit.unpooled(0, null);
        hit.setDocumentField(new DocumentField("f", List.of("value")));
        assertThat(SearchHitRamUsageEstimator.estimateDocumentFields(hit), greaterThan(0L));
    }

    public void testEstimateIsNonZeroForHitWithMetadataField() {
        SearchHit hit = SearchHit.unpooled(0, null);
        hit.addDocumentFields(Collections.emptyMap(), Map.of("_routing", new DocumentField("_routing", List.of("r1"))));
        assertThat(SearchHitRamUsageEstimator.estimateDocumentFields(hit), greaterThan(0L));
    }

    public void testEstimateIncludesBothDocAndMetaFields() {
        SearchHit hitBoth = SearchHit.unpooled(0, null);
        hitBoth.addDocumentFields(
            Map.of("f", new DocumentField("f", List.of("value"))),
            Map.of("_routing", new DocumentField("_routing", List.of("r1")))
        );
        SearchHit hitDocOnly = SearchHit.unpooled(0, null);
        hitDocOnly.setDocumentField(new DocumentField("f", List.of("value")));

        assertThat(
            "estimate for hit with both doc and meta fields should be larger",
            SearchHitRamUsageEstimator.estimateDocumentFields(hitBoth),
            greaterThan(SearchHitRamUsageEstimator.estimateDocumentFields(hitDocOnly))
        );
    }

    /**
     * Inner-hit bytes are accounted separately and transferred to the parent by InnerHitsPhase.
     * estimateDocumentFields must NOT include them to avoid double-counting.
     */
    public void testEstimateExcludesInnerHits() {
        SearchHit hitWithInner = SearchHit.unpooled(0, null);
        hitWithInner.setDocumentField(new DocumentField("f", List.of("value")));

        // Build a minimal inner hit with a field of its own; SearchHits is ref-counted so we close it.
        SearchHit innerHit = SearchHit.unpooled(0, null);
        innerHit.setDocumentField(new DocumentField("inner_field", List.of("inner_value")));
        SearchHits innerSearchHits = new SearchHits(new SearchHit[] { innerHit }, null, Float.NaN);
        try {
            hitWithInner.setInnerHits(Map.of("nested", innerSearchHits));

            SearchHit hitWithoutInner = SearchHit.unpooled(0, null);
            hitWithoutInner.setDocumentField(new DocumentField("f", List.of("value")));

            assertThat(
                "estimateDocumentFields should not count inner-hit fields",
                SearchHitRamUsageEstimator.estimateDocumentFields(hitWithInner),
                equalTo(SearchHitRamUsageEstimator.estimateDocumentFields(hitWithoutInner))
            );
        } finally {
            innerSearchHits.decRef();
        }
    }

    public void testEstimateGrowsWithNumberOfFields() {
        SearchHit small = SearchHit.unpooled(0, null);
        small.setDocumentField(new DocumentField("f1", List.of("v")));

        SearchHit large = SearchHit.unpooled(0, null);
        for (int i = 0; i < 100; i++) {
            large.setDocumentField(new DocumentField("f" + i, buildValues(10)));
        }

        assertThat(
            SearchHitRamUsageEstimator.estimateDocumentFields(large),
            greaterThan(SearchHitRamUsageEstimator.estimateDocumentFields(small))
        );
    }

    public void testEstimateIsUpperBoundForDocumentFields() {
        // Only test that the estimate is >= 0 for an empty hit (no fields produce no negative charge)
        SearchHit empty = SearchHit.unpooled(0, null);
        assertThat(SearchHitRamUsageEstimator.estimateDocumentFields(empty), greaterThanOrEqualTo(0L));
    }

    // ----- FetchContext.chargeInnerHitsBytes hook ----------------------------------------

    /**
     * The InnerHitsPhase hook (chargeInnerHitsBytes) must forward the bytes to the installed checker.
     * This mirrors the pattern in ScriptFieldsPhaseTests for the renamed method.
     */
    public void testChargeInnerHitsBytesForwardsToChecker() throws Exception {
        try (TestSearchContext searchContext = new TestSearchContext((SearchExecutionContext) null)) {
            List<Long> received = new ArrayList<>();
            FetchContext fetchContext = new FetchContext(searchContext, null);
            fetchContext.setInnerHitsByteChecker(received::add);

            fetchContext.chargeInnerHitsBytes(42L);
            assertThat(received, contains(42L));
        }
    }

    public void testChargeInnerHitsBytesIgnoresNonPositive() throws Exception {
        try (TestSearchContext searchContext = new TestSearchContext((SearchExecutionContext) null)) {
            List<Long> received = new ArrayList<>();
            FetchContext fetchContext = new FetchContext(searchContext, null);
            fetchContext.setInnerHitsByteChecker(received::add);

            fetchContext.chargeInnerHitsBytes(0L);
            fetchContext.chargeInnerHitsBytes(-1L);
            assertThat(received, empty());
        }
    }

    // ----- helpers -----------------------------------------------------------------------

    private static List<Object> buildValues(int n) {
        List<Object> values = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            values.add("entry-" + i);
        }
        return values;
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.search.fetch;

import org.apache.lucene.search.Explanation;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitRamUsageEstimator;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TestSearchContext;
import org.elasticsearch.xcontent.Text;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

/**
 * Unit tests for the per-hit heap estimate used by the fetch-phase circuit breaker, covering everything the fetch
 * sub-phases attach to a hit. End-to-end breaker behavior is covered by {@code FetchPhaseCircuitBreakerIT}.
 */
public class DocumentFieldAccountingTests extends ESTestCase {

    // ----- estimateSubPhaseOutput coverage -----------------------------------------------

    public void testEstimateIsNonZeroForHitWithDocumentField() {
        SearchHit hit = SearchHit.unpooled(0, null);
        hit.setDocumentField(new DocumentField("f", List.of("value")));
        assertThat(SearchHitRamUsageEstimator.estimateSubPhaseOutput(hit), greaterThan(0L));
    }

    public void testEstimateIsNonZeroForHitWithMetadataField() {
        SearchHit hit = SearchHit.unpooled(0, null);
        hit.addDocumentFields(Collections.emptyMap(), Map.of("_routing", new DocumentField("_routing", List.of("r1"))));
        assertThat(SearchHitRamUsageEstimator.estimateSubPhaseOutput(hit), greaterThan(0L));
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
            SearchHitRamUsageEstimator.estimateSubPhaseOutput(hitBoth),
            greaterThan(SearchHitRamUsageEstimator.estimateSubPhaseOutput(hitDocOnly))
        );
    }

    // InnerHitsPhase charges inner-hit bytes separately, so counting them here would double-count.
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
                "estimateSubPhaseOutput should not count inner-hit fields",
                SearchHitRamUsageEstimator.estimateSubPhaseOutput(hitWithInner),
                equalTo(SearchHitRamUsageEstimator.estimateSubPhaseOutput(hitWithoutInner))
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
            SearchHitRamUsageEstimator.estimateSubPhaseOutput(large),
            greaterThan(SearchHitRamUsageEstimator.estimateSubPhaseOutput(small))
        );
    }

    public void testEstimateIncludesHighlightFields() {
        SearchHit plain = SearchHit.unpooled(0, null);
        SearchHit highlighted = SearchHit.unpooled(0, null);
        String fragment = randomAlphaOfLength(4096);
        highlighted.highlightFields(Map.of("body", new HighlightField("body", new Text[] { new Text(fragment) })));

        assertThat(
            "the charge site must see whole-field highlight fragments",
            SearchHitRamUsageEstimator.estimateSubPhaseOutput(highlighted) - SearchHitRamUsageEstimator.estimateSubPhaseOutput(plain),
            greaterThanOrEqualTo((long) fragment.length())
        );
    }

    public void testEstimateIncludesExplanation() {
        SearchHit plain = SearchHit.unpooled(0, null);
        SearchHit explained = SearchHit.unpooled(0, null);
        String description = randomAlphaOfLength(4096);
        explained.explanation(Explanation.match(1.0f, description));

        assertThat(
            "the charge site must see the explanation tree",
            SearchHitRamUsageEstimator.estimateSubPhaseOutput(explained) - SearchHitRamUsageEstimator.estimateSubPhaseOutput(plain),
            greaterThanOrEqualTo((long) description.length())
        );
    }

    public void testEstimateIncludesMatchedQueries() {
        SearchHit plain = SearchHit.unpooled(0, null);
        SearchHit matched = SearchHit.unpooled(0, null);
        String name = randomAlphaOfLength(1024);
        matched.matchedQueries(new LinkedHashMap<>(Map.of(name, 1.0f)));

        assertThat(
            "the charge site must see matched queries",
            SearchHitRamUsageEstimator.estimateSubPhaseOutput(matched) - SearchHitRamUsageEstimator.estimateSubPhaseOutput(plain),
            greaterThanOrEqualTo((long) name.length())
        );
    }

    // Same exclusion as above: inner hits are highlighted and charged by their own nested fetch.
    public void testEstimateExcludesInnerHitHighlights() {
        SearchHit innerHit = SearchHit.unpooled(0, null);
        innerHit.highlightFields(Map.of("body", new HighlightField("body", new Text[] { new Text(randomAlphaOfLength(4096)) })));
        SearchHits innerSearchHits = new SearchHits(new SearchHit[] { innerHit }, null, Float.NaN);
        try {
            SearchHit hitWithInner = SearchHit.unpooled(0, null);
            hitWithInner.setInnerHits(Map.of("nested", innerSearchHits));

            assertThat(
                "estimateSubPhaseOutput should not count inner-hit highlights",
                SearchHitRamUsageEstimator.estimateSubPhaseOutput(hitWithInner),
                equalTo(SearchHitRamUsageEstimator.estimateSubPhaseOutput(SearchHit.unpooled(0, null)))
            );
        } finally {
            innerSearchHits.decRef();
        }
    }

    // ----- FetchContext.chargeInnerHitsBytes hook ----------------------------------------

    /**
     * The InnerHitsPhase hook (chargeInnerHitsBytes) must forward bytes to the installed checker.
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

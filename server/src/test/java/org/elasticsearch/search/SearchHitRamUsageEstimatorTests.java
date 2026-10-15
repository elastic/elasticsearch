/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search;

import org.apache.lucene.search.Explanation;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.Text;
import org.elasticsearch.xcontent.XContentString;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class SearchHitRamUsageEstimatorTests extends ESTestCase {

    public void testHighlightFragmentsAreCounted() {
        String fragment = randomAlphaOfLength(2048);
        SearchHit plain = SearchHit.unpooled(1);
        SearchHit highlighted = SearchHit.unpooled(1);
        highlighted.highlightFields(Map.of("body", new HighlightField("body", new Text[] { new Text(fragment) })));

        long delta = highlighted.ramBytesUsed() - plain.ramBytesUsed();
        assertThat(delta, greaterThanOrEqualTo((long) fragment.length()));
    }

    public void testNullFragmentsAreTolerated() {
        SearchHit hit = SearchHit.unpooled(1);
        hit.highlightFields(Map.of("body", new HighlightField("body", null)));
        assertThat(hit.ramBytesUsed(), greaterThanOrEqualTo(0L));
    }

    public void testEstimatingDoesNotMaterializeTheOtherViewOfAFragment() {
        byte[] utf8 = randomAlphaOfLength(64).getBytes(StandardCharsets.UTF_8);
        Text fromBytes = new Text(new XContentString.UTF8Bytes(utf8, 0, utf8.length));
        Text fromString = new Text(randomAlphaOfLength(64));
        SearchHit hit = SearchHit.unpooled(1);
        hit.highlightFields(Map.of("body", new HighlightField("body", new Text[] { fromBytes, fromString })));

        hit.ramBytesUsed();

        assertFalse(fromBytes.hasString());
        assertFalse(fromString.hasBytes());
    }

    public void testExplanationIsCounted() {
        SearchHit plain = SearchHit.unpooled(1);
        SearchHit explained = SearchHit.unpooled(1);
        String description = randomAlphaOfLength(2048);
        explained.explanation(Explanation.match(1.0f, description));

        long delta = explained.ramBytesUsed() - plain.ramBytesUsed();
        assertThat(delta, greaterThanOrEqualTo((long) description.length()));
    }

    public void testExplanationEstimateGrowsWithTreeSize() {
        SearchHit shallow = SearchHit.unpooled(1);
        shallow.explanation(Explanation.match(1.0f, "root"));

        SearchHit deep = SearchHit.unpooled(1);
        Explanation[] details = new Explanation[32];
        for (int i = 0; i < details.length; i++) {
            details[i] = Explanation.match(1.0f, "clause " + i, Explanation.match(1.0f, "term " + i));
        }
        deep.explanation(Explanation.match(1.0f, "root", details));

        assertThat(deep.ramBytesUsed(), greaterThan(shallow.ramBytesUsed()));
    }

    // A tree is as deep as the query nests, and recursion would overflow the stack well before this depth. The chain
    // stays under MAX_EXPLANATION_NODES on purpose, so the walk runs to completion rather than stopping at the cap.
    // If the cap is ever lowered, keep this in the tens of thousands, or it no longer outgrows a recursive walk.
    public void testDeeplyNestedExplanationDoesNotOverflowTheStack() {
        assertThat(estimateChainOf(SearchHitRamUsageEstimator.MAX_EXPLANATION_NODES / 2), greaterThan(0L));
    }

    // The cap is reachable without any sharing, which is what the javadoc claims. Both chains are longer than it and
    // share nothing, so each walk stops after the same MAX_EXPLANATION_NODES nodes and the extra node never shows up.
    public void testDistinctExplanationNodesReachTheNodeCap() {
        long justPastCap = estimateChainOf(SearchHitRamUsageEstimator.MAX_EXPLANATION_NODES + 1);
        long furtherPastCap = estimateChainOf(SearchHitRamUsageEstimator.MAX_EXPLANATION_NODES + 2);

        assertThat("the walk should stop at the cap, so the longer chain estimates the same", furtherPastCap, equalTo(justPastCap));
        assertThat(justPastCap, greaterThan(SearchHitRamUsageEstimator.EXPLANATION_NODE_CAP_PENALTY_BYTES));
    }

    // Sharing makes the node count grow exponentially while retained heap does not: 19 levels of pair-sharing expand
    // to 2^20 - 1 visits, well past MAX_EXPLANATION_NODES, from only 20 live objects.
    public void testSharedSubExplanationsStopAtTheNodeCap() {
        Explanation explanation = Explanation.match(1.0f, "leaf");
        for (int i = 0; i < 19; i++) {
            explanation = Explanation.match(1.0f, "level", explanation, explanation);
        }
        SearchHit hit = SearchHit.unpooled(1);
        hit.explanation(explanation);

        assertThat(hit.ramBytesUsed(), greaterThan(SearchHitRamUsageEstimator.EXPLANATION_NODE_CAP_PENALTY_BYTES));
    }

    // Explanation.match takes any Number, so the sizing must not assume the boxed float it carries in practice. The
    // upper bound guards the fallback against RamUsageEstimator.sizeOfObject, which bills an unknown object at 256.
    public void testNonFloatExplanationValueIsCounted() {
        Number value = randomFrom(new Number[] { 7, 7L, 7.0d, (short) 7, (byte) 7 });
        String description = randomAlphaOfLength(2048);
        SearchHit boxedFloat = SearchHit.unpooled(1);
        boxedFloat.explanation(Explanation.match(7.0f, description));
        SearchHit other = SearchHit.unpooled(1);
        other.explanation(Explanation.match(value, description));

        // Identical apart from the boxed value, and no Number box is smaller than a Float or more than 8 bytes larger.
        assertThat(other.ramBytesUsed(), greaterThanOrEqualTo(boxedFloat.ramBytesUsed()));
        assertThat(other.ramBytesUsed(), lessThanOrEqualTo(boxedFloat.ramBytesUsed() + 8));
    }

    // Builds a linear chain of distinct Explanation nodes and returns the estimate, so the chain is collectable on
    // return rather than being held across assertions.
    private static long estimateChainOf(int nodes) {
        Explanation explanation = Explanation.match(1.0f, "leaf");
        for (int i = 1; i < nodes; i++) {
            explanation = Explanation.match(1.0f, "level", explanation);
        }
        SearchHit hit = SearchHit.unpooled(1);
        hit.explanation(explanation);
        return hit.ramBytesUsed();
    }

    public void testMatchedQueriesAreCounted() {
        SearchHit plain = SearchHit.unpooled(1);
        SearchHit matched = SearchHit.unpooled(1);
        String name = randomAlphaOfLength(512);
        matched.matchedQueries(new LinkedHashMap<>(Map.of(name, 1.0f)));

        long delta = matched.ramBytesUsed() - plain.ramBytesUsed();
        assertThat(delta, greaterThanOrEqualTo((long) name.length()));
    }

    public void testMatchedQueriesEstimateGrowsWithNumberOfQueries() {
        SearchHit few = SearchHit.unpooled(1);
        few.matchedQueries(new LinkedHashMap<>(Map.of("q0", 1.0f)));

        SearchHit many = SearchHit.unpooled(1);
        Map<String, Float> names = new LinkedHashMap<>();
        for (int i = 0; i < 100; i++) {
            names.put("q" + i, (float) i);
        }
        many.matchedQueries(names);

        assertThat(many.ramBytesUsed(), greaterThan(few.ramBytesUsed()));
    }
}

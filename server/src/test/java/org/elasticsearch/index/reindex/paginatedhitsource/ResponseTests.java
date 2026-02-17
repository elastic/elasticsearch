/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.reindex.paginatedhitsource;

import org.elasticsearch.index.reindex.PaginatedHitSource;
import org.elasticsearch.index.reindex.PaginatedHitSource.BasicHit;
import org.elasticsearch.index.reindex.PaginatedHitSource.Hit;
import org.elasticsearch.index.reindex.PaginatedHitSource.Response;
import org.elasticsearch.index.reindex.PaginatedHitSource.SearchFailure;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ResponseTests extends ESTestCase {

    /**
     * Verifies that all values provided to the constructor are returned unchanged by their respective getters.
     */
    public void testConstructor() {
        boolean timedOut = randomBoolean();
        List<SearchFailure> failures = randomFailures();
        long totalHits = randomNonNegativeLong();
        List<PaginatedHitSource.Hit> hits = randomHits();
        String scrollId = randomAlphaOfLengthBetween(3, 20);
        Response response = new Response(timedOut, failures, totalHits, hits, scrollId);

        assertEquals(timedOut, response.isTimedOut());
        assertSame(failures, response.getFailures());
        assertEquals(totalHits, response.getTotalHits());
        assertSame(hits, response.getHits());
        assertEquals(scrollId, response.getScrollId());
    }

    /**
     * Verifies that the response correctly reports when it has timed out.
     */
    public void testIsTimedOut() {
        boolean timedOut = randomBoolean();
        Response response = new Response(
            timedOut,
            Collections.emptyList(),
            randomNonNegativeLong(),
            Collections.emptyList(),
            randomAlphaOfLengthBetween(3, 20)
        );
        assertEquals(timedOut, response.isTimedOut());
    }

    /**
     * Verifies that a (possibly empty) failures list is returned unchanged, preserving all failure entries
     */
    public void testGetFailures() {
        List<SearchFailure> failures = randomBoolean() ? Collections.emptyList() : randomFailures();
        Response response = new Response(
            randomBoolean(),
            failures,
            randomNonNegativeLong(),
            Collections.emptyList(),
            randomAlphaOfLengthBetween(3, 20)
        );
        assertSame(failures, response.getFailures());
        assertEquals(failures.size(), response.getFailures().size());
    }

    /**
     * Verifies that totalHits returned by the getter matches the value provided at construction time.
     */
    public void testGetTotalHits() {
        long totalHits = randomNonNegativeLong();
        Response response = new Response(
            randomBoolean(),
            Collections.emptyList(),
            totalHits,
            Collections.emptyList(),
            randomAlphaOfLengthBetween(3, 20)
        );
        assertEquals(totalHits, response.getTotalHits());
    }

    /**
     * Verifies that a (possible empty) hits list is returned unchanged and preserves all hit entries.
     */
    public void testEmptyHitsList() {
        List<Hit> hits = randomBoolean() ? Collections.emptyList() : randomHits();
        Response response = new Response(
            randomBoolean(),
            Collections.emptyList(),
            randomNonNegativeLong(),
            hits,
            randomAlphaOfLengthBetween(3, 20)
        );
        assertSame(hits, response.getHits());
        assertEquals(hits.size(), response.getHits().size());
    }

    /**
     * Verifies that the scroll id returned by the getter matches the value provided at construction time.
     */
    public void testGetScrollId() {
        String scrollId = randomAlphaOfLengthBetween(5, 30);
        Response response = new Response(
            randomBoolean(),
            Collections.emptyList(),
            randomNonNegativeLong(),
            Collections.emptyList(),
            scrollId
        );
        assertEquals(scrollId, response.getScrollId());
    }

    /**
     * Verifies that providing null values for optional collections is preserved and returned as-is by the getters.
     */
    public void testNullCollectionsArePreserved() {
        List<SearchFailure> failures = null;
        List<Hit> hits = null;
        Response response = new Response(randomBoolean(), failures, randomNonNegativeLong(), hits, randomAlphaOfLengthBetween(3, 20));
        assertNull(response.getFailures());
        assertNull(response.getHits());
    }

    private static List<SearchFailure> randomFailures() {
        int size = randomIntBetween(1, 5);
        List<SearchFailure> failures = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            failures.add(
                new SearchFailure(
                    new IllegalStateException(randomAlphaOfLengthBetween(5, 20)),
                    randomBoolean() ? randomAlphaOfLengthBetween(3, 10) : null,
                    randomBoolean() ? randomIntBetween(0, 10) : null,
                    randomBoolean() ? randomAlphaOfLengthBetween(3, 10) : null
                )
            );
        }
        return failures;
    }

    private static List<Hit> randomHits() {
        int size = randomIntBetween(1, 5);
        List<Hit> hits = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            hits.add(new BasicHit(randomAlphaOfLengthBetween(3, 10), randomAlphaOfLengthBetween(3, 10), randomNonNegativeLong()));
        }
        return hits;
    }
}

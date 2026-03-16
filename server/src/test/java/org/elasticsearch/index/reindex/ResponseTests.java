/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.reindex;

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
        List<SearchFailure> failures = randomBoolean() ? Collections.emptyList() : randomFailures();
        long totalHits = randomNonNegativeLong();
        List<PaginatedHitSource.Hit> hits = randomBoolean() ? Collections.emptyList() : randomHits();
        String scrollId = randomAlphaOfLengthBetween(3, 20);
        Response response = new Response(timedOut, failures, totalHits, hits, scrollId);

        assertEquals(timedOut, response.isTimedOut());
        assertSame(failures, response.getFailures());
        assertEquals(totalHits, response.getTotalHits());
        assertSame(hits, response.getHits());
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

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.execution.sample;

import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.elasticsearch.xpack.eql.execution.sample.SampleIterator.matchSamples;

public class SampleIteratorTests extends ESTestCase {

    public void testMatchSamples() {
        assertEquals(
            List.of(asSearchHitsList(2, 1, 3)),
            matchSamples(asList(asSearchHitsList(1, 1, 2), asSearchHitsList(1, 1, 1), asSearchHitsList(1, 1, 3)), 3, 1)
        );
        assertEquals(
            List.of(asSearchHitsList(1, 4, 5)),
            matchSamples(asList(asSearchHitsList(1, 2, 3), asSearchHitsList(4, 5), asSearchHitsList(4, 5, 3)), 3, 1)
        );
        assertEquals(
            List.of(asSearchHitsList(1, 2, 3)),
            matchSamples(asList(asSearchHitsList(1), asSearchHitsList(2), asSearchHitsList(3)), 3, 1)
        );
        assertTrue(matchSamples(asList(asSearchHitsList(3), asSearchHitsList(3), asSearchHitsList(3)), 3, 1).isEmpty());
        assertTrue(matchSamples(asList(asSearchHitsList(1), asSearchHitsList(1), asSearchHitsList(3)), 3, 1).isEmpty());
        assertTrue(matchSamples(asList(asSearchHitsList(1), asSearchHitsList(3), asSearchHitsList(3)), 3, 1).isEmpty());
        assertTrue(matchSamples(asList(asSearchHitsList(1, 1, 1), asSearchHitsList(1, 1, 1), asSearchHitsList(1, 1, 3)), 3, 1).isEmpty());

        // multiple matches per key
        List<List<SearchHit>> combinations = List.of(
            asSearchHitsList(1, 3, 4),
            asSearchHitsList(1, 4, 3),
            asSearchHitsList(2, 1, 3),
            asSearchHitsList(2, 1, 4),
            asSearchHitsList(2, 3, 1),
            asSearchHitsList(2, 3, 4),
            asSearchHitsList(2, 4, 1),
            asSearchHitsList(2, 4, 3),
            asSearchHitsList(3, 1, 4),
            asSearchHitsList(3, 4, 1)
        );
        for (int i = 1; i <= combinations.size(); i++) {
            assertEquals(
                combinations.subList(0, i),
                matchSamples(asList(asSearchHitsList(1, 2, 3), asSearchHitsList(1, 3, 4), asSearchHitsList(1, 3, 4)), 3, i)
            );
        }
        assertEquals(
            combinations,
            matchSamples(
                asList(asSearchHitsList(1, 2, 3), asSearchHitsList(1, 3, 4), asSearchHitsList(1, 3, 4)),
                3,
                combinations.size() + 1
            )
        );

    }

    private List<SearchHit> asSearchHitsList(Integer... docIds) {
        if (docIds == null || docIds.length == 0) {
            return emptyList();
        }
        List<SearchHit> searchHits = new ArrayList<>(docIds.length);
        for (Integer docId : docIds) {
            searchHits.add(new SearchHit(docId, docId.toString()));
        }

        return searchHits;
    }
}

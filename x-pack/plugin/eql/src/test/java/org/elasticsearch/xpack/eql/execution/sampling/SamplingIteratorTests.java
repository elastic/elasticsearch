/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.execution.sampling;

import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.elasticsearch.xpack.eql.execution.sampling.SamplingIterator.matchSampling;

public class SamplingIteratorTests extends ESTestCase {

    public void testMatchSampling() {
        assertEquals(
            asSearchHitsList(2, 1, 3),
            matchSampling(asList(asSearchHitsList(1, 1, 2), asSearchHitsList(1, 1, 1), asSearchHitsList(1, 1, 3)), 3)
        );
        assertEquals(
            asSearchHitsList(1, 4, 5),
            matchSampling(asList(asSearchHitsList(1, 2, 3), asSearchHitsList(4, 5), asSearchHitsList(4, 5, 3)), 3)
        );
        assertEquals(asSearchHitsList(1, 2, 3), matchSampling(asList(asSearchHitsList(1), asSearchHitsList(2), asSearchHitsList(3)), 3));
        assertNull(matchSampling(asList(asSearchHitsList(3), asSearchHitsList(3), asSearchHitsList(3)), 3));
        assertNull(matchSampling(asList(asSearchHitsList(1), asSearchHitsList(1), asSearchHitsList(3)), 3));
        assertNull(matchSampling(asList(asSearchHitsList(1), asSearchHitsList(3), asSearchHitsList(3)), 3));
        assertNull(matchSampling(asList(asSearchHitsList(1, 1, 1), asSearchHitsList(1, 1, 1), asSearchHitsList(1, 1, 3)), 3));
    }

    private List<SearchHit> asSearchHitsList(Integer... docIds) {
        if (docIds == null || docIds.length == 0) {
            return emptyList();
        }
        List<SearchHit> searchHits = new ArrayList<>(docIds.length);
        for (Integer docId : docIds) {
            searchHits.add(new SearchHit(docId, docId.toString(), null, null));
        }

        return searchHits;
    }
}

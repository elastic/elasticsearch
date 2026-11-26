/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License 2.0.
 */

package org.elasticsearch.xpack.eql.execution.sample;

import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.eql.action.EqlSearchResponse;
import org.elasticsearch.xpack.eql.execution.sequence.SequenceKey;
import org.elasticsearch.xpack.eql.session.Payload;

import java.util.Arrays;
import java.util.List;

public class SampleIteratorFetchTests extends ESTestCase {

    public void testAMissingFetchedHitDropsThatSampleAndReportsIt() {
        SearchHit keptA = new SearchHit(1, "a");
        SearchHit keptB = new SearchHit(2, "b");
        SearchHit dropped = new SearchHit(3, "c");
        Sample keep = sample("keep");
        Sample drop = sample("drop");
        SampleIterator iterator = new SampleIterator(null, List.of(), 10, null, NoopCircuitBreaker.INSTANCE, 1, true);

        Payload payload = iterator.samplesFromFetch(List.of(keep, drop), List.of(List.of(keptA, keptB), Arrays.asList(dropped, null)));

        assertEquals(1, payload.values().size());
        assertEquals(List.of("keep"), ((EqlSearchResponse.Sequence) payload.values().get(0)).joinKeys());
        assertEquals(1, payload.shardFailures().size());
        assertFalse(keptA.hasReferences());
        assertFalse(dropped.hasReferences());
    }

    private static Sample sample(String key) {
        SearchHit hit = new SearchHit(1, key);
        try {
            return new Sample(new SequenceKey(key), List.of(hit));
        } finally {
            hit.decRef();
        }
    }
}

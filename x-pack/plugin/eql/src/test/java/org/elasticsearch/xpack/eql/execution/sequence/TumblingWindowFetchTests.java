/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License 2.0.
 */

package org.elasticsearch.xpack.eql.execution.sequence;

import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.eql.action.EqlSearchResponse;
import org.elasticsearch.xpack.eql.execution.assembler.BoxedQueryRequest;
import org.elasticsearch.xpack.eql.execution.assembler.SequenceCriterion;
import org.elasticsearch.xpack.eql.execution.search.HitReference;
import org.elasticsearch.xpack.eql.execution.search.Ordinal;
import org.elasticsearch.xpack.eql.execution.search.Timestamp;
import org.elasticsearch.xpack.eql.session.Payload;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;

public class TumblingWindowFetchTests extends ESTestCase {

    public void testAMissingEventStageIsKeptWhenEveryPositiveHitWasFetched() {
        Payload payload = window(new boolean[] { false, true, false }, false, true).sequencesFromFetch(
            completed(sequence("k", "1", "3")),
            List.of(List.of(new SearchHit(1, "e0"), new SearchHit(3, "e2")))
        );
        assertEquals(1, payload.values().size());
        assertEquals(0, payload.shardFailures().size());
        EqlSearchResponse.Sequence sequence = (EqlSearchResponse.Sequence) payload.values().get(0);
        assertEquals(3, sequence.events().size());
        assertFalse(sequence.events().get(0).missing());
        assertTrue(sequence.events().get(1).missing());
        assertFalse(sequence.events().get(2).missing());
    }

    public void testADescendingFetchDropsTheSequenceWhoseDocumentIsMissing() {
        Sequence early = sequence("early", "1", "2");
        Sequence late = sequence("late", "3", "4");
        Payload payload = window(new boolean[] { false, false }, true, true).sequencesFromFetch(
            completed(early, late),
            List.of(Arrays.asList(new SearchHit(1, "late"), null), List.of(new SearchHit(2, "early"), new SearchHit(3, "early-2")))
        );
        assertEquals(1, payload.values().size());
        assertEquals(List.of("early"), ((EqlSearchResponse.Sequence) payload.values().get(0)).joinKeys());
        assertEquals(1, payload.shardFailures().size());
    }

    public void testDisallowingPartialSequencesReturnsNoneWhenAFetchMisses() {
        SearchHit kept = new SearchHit(1, "kept");
        Payload payload = window(new boolean[] { false, false }, false, false).sequencesFromFetch(
            completed(sequence("kept", "1", "2"), sequence("lost", "3", "4")),
            List.of(List.of(kept, new SearchHit(2, "kept-2")), Arrays.asList(new SearchHit(3, "lost"), null))
        );
        assertEquals(0, payload.values().size());
        assertEquals(1, payload.shardFailures().size());
        assertFalse(kept.hasReferences());
    }

    private static List<Sequence> completed(Sequence... sequences) {
        SequenceMatcher matcher = new SequenceMatcher(
            sequences[0].hits().size(),
            false,
            TimeValue.MINUS_ONE,
            null,
            new boolean[sequences[0].hits().size()],
            NoopCircuitBreaker.INSTANCE
        );
        for (Sequence sequence : sequences) {
            matcher.addToCompleted(sequence);
        }
        return matcher.completed();
    }

    private static TumblingWindow window(boolean[] missing, boolean descending, boolean allowPartialSequenceResults) {
        List<SequenceCriterion> criteria = new ArrayList<>();
        for (int i = 0; i < missing.length; i++) {
            criteria.add(criterion(i, i == 0 && descending));
        }
        return new TumblingWindow(
            null,
            criteria,
            null,
            new SequenceMatcher(missing.length, false, TimeValue.MINUS_ONE, null, missing, NoopCircuitBreaker.INSTANCE),
            List.of(),
            true,
            allowPartialSequenceResults
        );
    }

    private static SequenceCriterion criterion(int stage, boolean descending) {
        return new SequenceCriterion(
            stage,
            new BoxedQueryRequest(
                () -> SearchSourceBuilder.searchSource().size(10).query(matchAllQuery()),
                "@timestamp",
                List.of(),
                Set.of()
            ),
            List.of(),
            null,
            null,
            null,
            descending,
            false
        );
    }

    private static Sequence sequence(String key, String startTimestamp, String endTimestamp) {
        Sequence sequence = new Sequence(
            new SequenceKey(key),
            2,
            0,
            new Ordinal(Timestamp.of(startTimestamp), null, 1L),
            new HitReference("logs", key)
        );
        sequence.putMatch(1, new Ordinal(Timestamp.of(endTimestamp), null, 2L), new HitReference("logs", key + "-2"));
        return sequence;
    }
}

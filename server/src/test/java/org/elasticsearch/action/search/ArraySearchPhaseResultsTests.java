/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.action.search;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.fetch.FetchSearchResult;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.search.lookup.Source;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A failed phase releases the collection while its shard requests are still in flight. These tests cover a result
 * arriving before that release, during it, and after it.
 */
public class ArraySearchPhaseResultsTests extends ESTestCase {

    public void testCollectedResultsAreReleasedOnClose() {
        FetchSearchResult result = fetchResult(0);
        try (ArraySearchPhaseResults<FetchSearchResult> results = new ArraySearchPhaseResults<>(1)) {
            results.consumeResult(result, () -> {});
            assertTrue(result.hasReferences());
            result.decRef();
            assertTrue("the collection holds the only remaining reference", result.hasReferences());
        }
        assertFalse(result.hasReferences());
    }

    public void testResultArrivingAfterCloseIsNotReferenced() {
        ArraySearchPhaseResults<FetchSearchResult> results = new ArraySearchPhaseResults<>(2);
        results.close();

        FetchSearchResult late = fetchResult(1);
        AtomicBoolean nextRan = new AtomicBoolean();
        results.consumeResult(late, () -> nextRan.set(true));
        assertTrue("the shard still has to be counted down", nextRan.get());
        assertNull("a result that nothing will release must not be collected", results.getAtomicArray().get(1));

        late.decRef();
        assertFalse("the caller's reference must be the last one", late.hasReferences());
    }

    public void testResultIsReleasedWhenTheContinuationCloses() {
        FetchSearchResult result = fetchResult(0);
        ArraySearchPhaseResults<FetchSearchResult> results = new ArraySearchPhaseResults<>(1);
        // baseline, like testCollectedResultsAreReleasedOnClose: a phase failure raised from the continuation is the
        // usual way the collection gets closed, and by then the result is already recorded
        results.consumeResult(result, results::close);

        result.decRef();
        assertFalse(result.hasReferences());
    }

    public void testCloseBetweenStoringAndReferencingStillReleases() {
        ArraySearchPhaseResults<SearchPhaseResult> results = new ArraySearchPhaseResults<>(1);
        RefCounted resultRefs = AbstractRefCounted.of(() -> {});
        SearchPhaseResult result = new SearchPhaseResult() {
            // consumeResult stores the result and then calls this to reference it, so closing here lands between
            // the two. Reaching that gap needs no hook in production code, and
            // testConcurrentConsumeAndCloseReleasesEveryResult only gets there by luck.
            @Override
            public void incRef() {
                results.close();
                resultRefs.incRef();
            }

            @Override
            public boolean tryIncRef() {
                return resultRefs.tryIncRef();
            }

            @Override
            public boolean decRef() {
                return resultRefs.decRef();
            }

            @Override
            public boolean hasReferences() {
                return resultRefs.hasReferences();
            }

            @Override
            public void writeTo(StreamOutput out) {}
        };
        result.setShardIndex(0);

        results.consumeResult(result, () -> {});

        result.decRef();
        assertFalse(result.hasReferences());
    }

    public void testConcurrentConsumeAndCloseReleasesEveryResult() {
        // realistic race coverage for the same gap, from several threads at once; repeated because a single round
        // usually misses it
        for (int round = 0; round < 50; round++) {
            int numShards = randomIntBetween(2, 8);
            List<FetchSearchResult> shardResults = new ArrayList<>(numShards);
            for (int i = 0; i < numShards; i++) {
                shardResults.add(fetchResult(i));
            }

            ArraySearchPhaseResults<FetchSearchResult> results = new ArraySearchPhaseResults<>(numShards);
            startInParallel(numShards + 1, i -> {
                if (i == numShards) {
                    results.close();
                } else {
                    results.consumeResult(shardResults.get(i), () -> {});
                }
            });

            for (FetchSearchResult result : shardResults) {
                result.decRef();
                assertFalse(result.hasReferences());
            }
        }
    }

    private static FetchSearchResult fetchResult(int shardIndex) {
        FetchSearchResult result = new FetchSearchResult(
            new ShardSearchContextId("", shardIndex),
            new SearchShardTarget("node", new ShardId("index", "uuid", shardIndex), null)
        );
        result.setShardIndex(shardIndex);
        SearchHit hit = new SearchHit(0, "id-" + shardIndex);
        hit.sourceRef(Source.fromMap(Map.of("f", "v"), XContentType.JSON).internalSourceRef());
        result.shardResult(new SearchHits(new SearchHit[] { hit }, new TotalHits(1, TotalHits.Relation.EQUAL_TO), 1.0f), null);
        return result;
    }
}

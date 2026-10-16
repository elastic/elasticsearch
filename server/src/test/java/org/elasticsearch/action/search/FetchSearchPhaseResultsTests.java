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
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.fetch.FetchSearchResult;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.action.search.FetchSearchPhaseTests.requestBreaker;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class FetchSearchPhaseResultsTests extends ESTestCase {

    public void testChargeIsHeldUntilTheResultsAreReleased() {
        CircuitBreaker breaker = requestBreaker("1gb");
        FetchSearchResult result = fetchResult(0, 3);
        try (FetchSearchPhaseResults results = new FetchSearchPhaseResults(1, breaker)) {
            long expected = hitBytes(result);
            assertThat(expected, greaterThan(0L));

            results.reserve(result);
            assertThat(breaker.getUsed(), equalTo(expected));

            // The hits outlive the fetch phase, so the charge only goes back when the results are released.
            results.close();
            assertThat(breaker.getUsed(), equalTo(0L));
        } finally {
            result.decRef();
        }
    }

    public void testChargeAccumulatesAcrossShards() {
        CircuitBreaker breaker = requestBreaker("1gb");
        int numShards = randomIntBetween(2, 5);
        List<FetchSearchResult> shardResults = new ArrayList<>(numShards);
        try (FetchSearchPhaseResults results = new FetchSearchPhaseResults(numShards, breaker)) {
            long expected = 0L;
            for (int i = 0; i < numShards; i++) {
                FetchSearchResult result = fetchResult(i, randomIntBetween(1, 4));
                shardResults.add(result);
                expected += hitBytes(result);
                results.reserve(result);
                assertThat(breaker.getUsed(), equalTo(expected));
            }
            results.close();
            assertThat(breaker.getUsed(), equalTo(0L));
        } finally {
            shardResults.forEach(FetchSearchResult::decRef);
        }
    }

    public void testShardThatTripsTheBreakerIsNotCharged() {
        // Small enough that the first shard fits and the second one does not.
        CircuitBreaker breaker = requestBreaker("2kb");
        FetchSearchResult first = fetchResult(0, 1);
        FetchSearchResult second = fetchResult(1, 4);
        try (FetchSearchPhaseResults results = new FetchSearchPhaseResults(2, breaker)) {
            results.reserve(first);
            long afterFirst = breaker.getUsed();
            assertThat(afterFirst, greaterThan(0L));

            expectThrows(CircuitBreakingException.class, () -> results.reserve(second));
            assertThat(breaker.getUsed(), equalTo(afterFirst));

            results.close();
            assertThat(breaker.getUsed(), equalTo(0L));
        } finally {
            first.decRef();
            second.decRef();
        }
    }

    public void testChargeHandedOverByTheChunkedPathIsTakenOverNotChargedAgain() {
        CircuitBreaker breaker = requestBreaker("1gb");
        FetchSearchResult handedOver = fetchResult(0, 1);
        // What the chunked path does: it charged for these hits while accumulating them, then handed the charge over.
        long charged = 4096L;
        breaker.addWithoutBreaking(charged);
        handedOver.setCoordinatorSearchHitsSizeBytes(charged, breaker);
        try (FetchSearchPhaseResults results = new FetchSearchPhaseResults(1, breaker)) {
            results.reserve(handedOver);
            assertThat("reserve must not estimate these hits a second time", breaker.getUsed(), equalTo(charged));
            assertFalse("the collection owns the charge now", handedOver.isChargedOnCoordinator());

            results.close();
            assertThat(breaker.getUsed(), equalTo(0L));
        } finally {
            handedOver.decRef();
        }
        // Releasing the result gave nothing back, since it no longer holds a charge.
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    public void testTransferCoversAChargeHandedOverByTheChunkedPath() {
        CircuitBreaker breaker = requestBreaker("1gb");
        FetchSearchResult handedOver = fetchResult(0, 1);
        long charged = 4096L;
        breaker.addWithoutBreaking(charged);
        handedOver.setCoordinatorSearchHitsSizeBytes(charged, breaker);
        try (FetchSearchPhaseResults results = new FetchSearchPhaseResults(1, breaker)) {
            results.reserve(handedOver);

            Releasable charge = results.transferCharge();
            assertThat("the chunked route's charge has to reach the response too", charge, notNullValue());
            results.close();
            assertThat(breaker.getUsed(), equalTo(charged));

            charge.close();
            assertThat(breaker.getUsed(), equalTo(0L));
        } finally {
            handedOver.decRef();
        }
    }

    public void testChargeSumsAcrossChunkedAndEstimatedShards() {
        // The route is picked per data node, so a rolling upgrade puts both kinds of shard in one fetch.
        CircuitBreaker breaker = requestBreaker("1gb");
        FetchSearchResult handedOver = fetchResult(0, 1);
        long charged = 4096L;
        breaker.addWithoutBreaking(charged);
        handedOver.setCoordinatorSearchHitsSizeBytes(charged, breaker);
        FetchSearchResult estimated = fetchResult(1, 2);
        try (FetchSearchPhaseResults results = new FetchSearchPhaseResults(2, breaker)) {
            results.reserve(handedOver);
            results.reserve(estimated);
            long expected = charged + hitBytes(estimated);
            assertThat(breaker.getUsed(), equalTo(expected));

            Releasable charge = results.transferCharge();
            results.close();
            assertThat("one charge covers both routes", breaker.getUsed(), equalTo(expected));

            charge.close();
            assertThat(breaker.getUsed(), equalTo(0L));
        } finally {
            handedOver.decRef();
            estimated.decRef();
        }
    }

    public void testChunkedShardArrivingAfterReleaseGivesItsChargeStraightBack() {
        CircuitBreaker breaker = requestBreaker("1gb");
        FetchSearchResult inFlight = fetchResult(0, 1);
        long charged = 4096L;
        breaker.addWithoutBreaking(charged);
        inFlight.setCoordinatorSearchHitsSizeBytes(charged, breaker);
        try (FetchSearchPhaseResults results = new FetchSearchPhaseResults(1, breaker)) {
            // A phase failure released the collection while this shard was still in flight.
            results.close();

            results.reserve(inFlight);
            assertThat("reserve has to give back a charge it took over but cannot hold", breaker.getUsed(), equalTo(0L));
            assertFalse(inFlight.isChargedOnCoordinator());
        } finally {
            inFlight.decRef();
        }
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    public void testShardArrivingAfterReleaseGivesItsChargeStraightBack() {
        CircuitBreaker breaker = requestBreaker("1gb");
        FetchSearchResult early = fetchResult(0, 2);
        FetchSearchResult inFlight = fetchResult(1, 2);
        try (FetchSearchPhaseResults results = new FetchSearchPhaseResults(2, breaker)) {
            results.reserve(early);
            // A phase failure releases the collection while the second shard is still in flight.
            results.close();
            assertThat(breaker.getUsed(), equalTo(0L));

            results.reserve(inFlight);
            assertThat(breaker.getUsed(), equalTo(0L));
        } finally {
            early.decRef();
            inFlight.decRef();
        }
    }

    public void testTransferHandsTheChargeToTheCallerAndClosesBecomeNoOps() {
        CircuitBreaker breaker = requestBreaker("1gb");
        FetchSearchResult result = fetchResult(0, 3);
        try (FetchSearchPhaseResults results = new FetchSearchPhaseResults(1, breaker)) {
            long expected = hitBytes(result);
            results.reserve(result);

            Releasable charge = results.transferCharge();
            assertThat(charge, notNullValue());
            // The caller now owns the charge, so close() must not also release it.
            assertThat(breaker.getUsed(), equalTo(expected));
            results.close();
            assertThat(breaker.getUsed(), equalTo(expected));

            charge.close();
            assertThat(breaker.getUsed(), equalTo(0L));
        } finally {
            result.decRef();
        }
    }

    public void testTransferWithNothingReservedReturnsNull() {
        CircuitBreaker breaker = requestBreaker("1gb");
        try (FetchSearchPhaseResults results = new FetchSearchPhaseResults(1, breaker)) {
            assertThat(results.transferCharge(), nullValue());
            results.close();
            assertThat(breaker.getUsed(), equalTo(0L));
        }
    }

    public void testShardArrivingAfterTransferGivesItsChargeStraightBack() {
        CircuitBreaker breaker = requestBreaker("1gb");
        FetchSearchResult early = fetchResult(0, 2);
        FetchSearchResult inFlight = fetchResult(1, 2);
        try (FetchSearchPhaseResults results = new FetchSearchPhaseResults(2, breaker)) {
            long expected = hitBytes(early);
            results.reserve(early);
            Releasable charge = results.transferCharge();

            // A shard that was still in flight when the response was built must not add to the transferred charge.
            results.reserve(inFlight);
            assertThat(breaker.getUsed(), equalTo(expected));

            charge.close();
            assertThat(breaker.getUsed(), equalTo(0L));
        } finally {
            early.decRef();
            inFlight.decRef();
        }
    }

    public void testResultWithoutHitsChargesNothing() {
        CircuitBreaker breaker = requestBreaker("1gb");
        FetchSearchResult result = fetchResult(0, 0);
        try (FetchSearchPhaseResults results = new FetchSearchPhaseResults(1, breaker)) {
            results.reserve(result);
            assertThat(breaker.getUsed(), equalTo(0L));
            results.close();
            assertThat(breaker.getUsed(), equalTo(0L));
        } finally {
            result.decRef();
        }
    }

    public void testCloseIsIdempotent() {
        CircuitBreaker breaker = requestBreaker("1gb");
        FetchSearchResult result = fetchResult(0, 2);
        try (FetchSearchPhaseResults results = new FetchSearchPhaseResults(1, breaker)) {
            results.reserve(result);
            results.close();
            results.close();
            assertThat(breaker.getUsed(), equalTo(0L));
        } finally {
            result.decRef();
        }
    }

    private static long hitBytes(FetchSearchResult result) {
        long bytes = 0L;
        for (SearchHit hit : result.hits().getHits()) {
            bytes += hit.ramBytesUsed();
        }
        return bytes;
    }

    private static FetchSearchResult fetchResult(int shardIndex, int numHits) {
        SearchHit[] hits = new SearchHit[numHits];
        for (int i = 0; i < numHits; i++) {
            hits[i] = new SearchHit(i).sourceRef(new BytesArray(randomAlphaOfLength(randomIntBetween(64, 512))));
        }
        FetchSearchResult result = new FetchSearchResult();
        result.setShardIndex(shardIndex);
        result.shardResult(new SearchHits(hits, new TotalHits(numHits, TotalHits.Relation.EQUAL_TO), 1.0F), null);
        return result;
    }
}

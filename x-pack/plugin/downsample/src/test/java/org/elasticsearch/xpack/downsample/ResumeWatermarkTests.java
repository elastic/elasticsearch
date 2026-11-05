/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.nullValue;

public class ResumeWatermarkTests extends ESTestCase {

    public void testNothingAddedYet() {
        assertThat(watermark(null).current(), nullValue());
        assertThat(watermark(tsid(0)).current(), equalTo(tsid(0)));
    }

    /**
     * Before the first bulk is sent the resume point is the first bucket of the bulk under construction: nothing was written yet,
     * and a resumed task re-reads that series. Once sent and confirmed, the resume point is the last bucket sent, inclusive.
     */
    public void testBulkUnderConstructionAndConfirmed() {
        ResumeWatermark watermark = watermark(null);
        watermark.addedTsid(tsid(1));
        watermark.addedTsid(tsid(2));
        assertThat(watermark.current(), equalTo(tsid(1)));
        watermark.preparingBulkRequest(1);
        assertThat(watermark.current(), equalTo(tsid(1)));
        watermark.succeeded(1);
        assertThat(watermark.current(), equalTo(tsid(2)));
        watermark.addedTsid(tsid(3));
        assertThat(watermark.current(), equalTo(tsid(3)));
    }

    /**
     * A resumed task already knows everything below its resume point is indexed, so that is its watermark until it adds a bucket.
     */
    public void testStartsFromTheResumePoint() {
        ResumeWatermark watermark = watermark(tsid(0));
        assertThat(watermark.current(), equalTo(tsid(0)));
        bulk(watermark, 1, 1, 2);
        assertThat(watermark.current(), equalTo(tsid(1)));
        watermark.succeeded(1);
        assertThat(watermark.current(), equalTo(tsid(2)));
    }

    /**
     * A later bulk completing first must not move the resume point past the earlier one still in flight.
     */
    public void testLaterBulkCompletesFirst() {
        ResumeWatermark watermark = watermark(null);
        bulk(watermark, 1, 1, 2);
        bulk(watermark, 2, 3, 4);
        bulk(watermark, 3, 5, 6);
        watermark.succeeded(3);
        watermark.succeeded(2);
        assertThat(watermark.current(), equalTo(tsid(1)));
        watermark.succeeded(1);
        assertThat(watermark.current(), equalTo(tsid(6)));
    }

    /**
     * A failed bulk is never confirmed, so the resume point stays at its first bucket no matter how many later bulks succeed.
     */
    public void testFailedBulkPinsTheWatermark() {
        ResumeWatermark watermark = watermark(null);
        bulk(watermark, 1, 1, 2);
        bulk(watermark, 2, 3, 4);
        bulk(watermark, 3, 5, 6);
        watermark.succeeded(1);
        watermark.succeeded(3);
        assertThat(watermark.current(), equalTo(tsid(3)));
        bulk(watermark, 4, 7, 8);
        watermark.succeeded(4);
        assertThat(watermark.current(), equalTo(tsid(3)));
    }

    /**
     * A series can span bulks. The resume point is then the series itself, whose already indexed buckets are re-read.
     */
    public void testSeriesSpanningBulks() {
        ResumeWatermark watermark = watermark(null);
        bulk(watermark, 1, 1, 2, 2);
        bulk(watermark, 2, 2, 2, 3);
        watermark.succeeded(1);
        assertThat(watermark.current(), equalTo(tsid(2)));
        watermark.succeeded(2);
        assertThat(watermark.current(), equalTo(tsid(3)));
    }

    /**
     * Bulks are sent in order but complete in a random order, with some of them failing. The resume point must always be the
     * first {@code _tsid} of the oldest bulk that is not confirmed, or the last {@code _tsid} sent when all are confirmed.
     */
    public void testRandomCompletionOrder() {
        ResumeWatermark watermark = watermark(null);
        int bulks = between(1, 50);
        List<Integer> failed = new ArrayList<>();
        for (int id = 1; id <= bulks; id++) {
            // Bulk id holds the series 2 * id - 1 and 2 * id
            bulk(watermark, id, 2 * id - 1, 2 * id);
            if (rarely()) {
                failed.add(id);
            }
        }
        List<Integer> toComplete = new ArrayList<>();
        for (int id = 1; id <= bulks; id++) {
            if (failed.contains(id) == false) {
                toComplete.add(id);
            }
        }
        // Some bulks may still be in flight when the task is interrupted
        int inFlight = between(0, toComplete.size());
        List<Integer> completionOrder = randomSubsetOf(toComplete.size() - inFlight, toComplete);
        Set<Integer> completed = new java.util.HashSet<>();
        for (int id : completionOrder) {
            watermark.succeeded(id);
            completed.add(id);
            assertThat(watermark.current(), equalTo(expectedWatermark(bulks, completed)));
        }
    }

    private static BytesRef expectedWatermark(int bulks, Set<Integer> completed) {
        for (int id = 1; id <= bulks; id++) {
            if (completed.contains(id) == false) {
                return tsid(2 * id - 1);
            }
        }
        return tsid(2 * bulks);
    }

    /**
     * Bulks are confirmed from another thread while the scan thread adds buckets, sends bulks and reads the watermark, with no lock
     * between the two. A read may be outdated, but it must never return a {@code _tsid} past the first bucket of a bulk that is
     * not confirmed, and the watermark must never move backwards.
     */
    public void testConcurrentConfirmations() throws Exception {
        ResumeWatermark watermark = watermark(null);
        int bulks = between(100, 2000);
        // Bulks sent and to be confirmed, handed from the scan thread to the confirmer
        Queue<Integer> toConfirm = new ConcurrentLinkedQueue<>();
        // A bulk is marked here before it is confirmed, so a bulk that is not marked when the scan thread looks is not confirmed
        Set<Integer> confirmed = ConcurrentHashMap.newKeySet();
        AtomicBoolean allSent = new AtomicBoolean();
        // The randomness of the test is not shared with another thread
        Random confirmerRandom = new Random(random().nextLong());
        Thread confirmer = new Thread(() -> {
            List<Integer> pending = new ArrayList<>();
            while (true) {
                for (Integer id = toConfirm.poll(); id != null; id = toConfirm.poll()) {
                    pending.add(id);
                }
                if (pending.isEmpty()) {
                    if (allSent.get() && toConfirm.isEmpty()) {
                        return;
                    }
                    Thread.onSpinWait();
                    continue;
                }
                int id = pending.remove(confirmerRandom.nextInt(pending.size()));
                confirmed.add(id);
                watermark.succeeded(id);
            }
        }, "confirmer");
        confirmer.start();
        try {
            BytesRef previous = null;
            for (int id = 1; id <= bulks; id++) {
                // Bulk id holds the series 2 * id - 1 and 2 * id
                bulk(watermark, id, 2 * id - 1, 2 * id);
                if (rarely() == false) {
                    toConfirm.add(id);
                }
                if (randomBoolean()) {
                    BytesRef current = watermark.current();
                    // The bound is computed after the read: a bulk not marked by now was certainly unconfirmed when the read happened
                    assertThat(current, lessThanOrEqualTo(expectedWatermark(id, confirmed)));
                    if (previous != null) {
                        assertThat(current, greaterThanOrEqualTo(previous));
                    }
                    previous = current;
                }
            }
        } finally {
            allSent.set(true);
            confirmer.join();
        }
        assertThat(watermark.current(), equalTo(expectedWatermark(bulks, confirmed)));
    }

    /**
     * The watermark is published periodically, but only once it moved since the last publication, only when no publication is in
     * flight, and at most once per interval.
     */
    public void testPublishesOnlyWhenMovedAndIntervalElapsed() {
        long interval = between(1, 1000);
        ResumeWatermark watermark = new ResumeWatermark(null, interval, 0);
        // Nothing to publish before a bucket is added
        assertThat(watermark.pollForPublication(interval), nullValue());

        bulk(watermark, 1, 1);
        // The interval starts at construction, since the task publishes its resume point when it starts
        assertThat(watermark.pollForPublication(interval - 1), nullValue());
        assertThat(watermark.pollForPublication(interval), equalTo(tsid(1)));
        // In flight: not offered again, even though it moved in the meantime
        watermark.succeeded(1);
        bulk(watermark, 2, 2);
        assertThat(watermark.pollForPublication(2 * interval), nullValue());

        watermark.publicationSucceeded(tsid(1), interval);
        assertThat(watermark.pollForPublication(2 * interval - 1), nullValue());
        assertThat(watermark.pollForPublication(2 * interval), equalTo(tsid(2)));
        watermark.publicationSucceeded(tsid(2), 2 * interval);
        // Not moved since: nothing to publish however much time passes
        assertThat(watermark.pollForPublication(10 * interval), nullValue());
    }

    public void testFailedPublicationIsRetriedAfterTheInterval() {
        long interval = between(1, 1000);
        ResumeWatermark watermark = new ResumeWatermark(null, interval, 0);
        bulk(watermark, 1, 1);
        assertThat(watermark.pollForPublication(interval), equalTo(tsid(1)));
        watermark.publicationFailed(interval);
        assertThat(watermark.pollForPublication(2 * interval - 1), nullValue());
        assertThat(watermark.pollForPublication(2 * interval), equalTo(tsid(1)));
    }

    /**
     * A resumed task publishes its resume point when it starts, so the resume point is not offered for publication again.
     */
    public void testResumePointIsNotRepublished() {
        long interval = between(1, 1000);
        ResumeWatermark watermark = new ResumeWatermark(tsid(0), interval, 0);
        assertThat(watermark.pollForPublication(10 * interval), nullValue());
        bulk(watermark, 1, 1);
        assertThat(watermark.pollForPublication(10 * interval), equalTo(tsid(1)));
    }

    /** Adds the buckets of the given series, in order, and cuts them into a bulk with the given execution id. */
    private static void bulk(ResumeWatermark watermark, long executionId, int... series) {
        for (int s : series) {
            watermark.addedTsid(tsid(s));
        }
        watermark.preparingBulkRequest(executionId);
    }

    /** A watermark whose publications are never throttled, for the tests of how it moves. */
    private static ResumeWatermark watermark(BytesRef resumedFrom) {
        return new ResumeWatermark(resumedFrom, 0, 0);
    }

    /** A {@code _tsid} that sorts by id, for ids below 100000. */
    private static BytesRef tsid(int id) {
        assert id < 100000 : "ids must sort as text";
        return new BytesRef(String.format(java.util.Locale.ROOT, "tsid-%05d", id));
    }
}

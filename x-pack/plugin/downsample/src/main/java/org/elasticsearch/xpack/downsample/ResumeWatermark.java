/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.core.Nullable;

import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Tracks the {@code _tsid} a shard downsampling task can safely be resumed from.
 * <p>
 * The indexer sends bulks concurrently and they complete in any order. In order to find a safe {@code _tsid} to resume from we need:
 * - if we have bulk requests that have not successfully completed, the first {@code _tsid} of such a request.
 * - if all bulk requests have successfully completed:
 *   - the resume point is the first {@code _tsid} of the bulk under construction, or if there is no bulk request under construction
 *   - the last {@code _tsid} sent
 * Buckets must be added and bulks must be sent in {@code _tsid} order, which holds because both are done by the single thread
 * scanning the shard. Confirmations may arrive from any thread and in any order.
 */
final class ResumeWatermark {

    /** Bulks that were sent but their success is confirmed yet. */
    private final ConcurrentSkipListMap<Long, BytesRef> unconfirmedTsidsByRequestId = new ConcurrentSkipListMap<>();
    /** The {@code _tsid} of the first bucket added since the last bulk was cut, or {@code null} if none was added since. */
    @Nullable
    private BytesRef firstOfBulkUnderConstruction;
    /** The {@code _tsid} of the last bucket added, or the resume point of the task if none was added yet. */
    @Nullable
    private BytesRef lastAdded;
    // only used for assertions
    private Thread scanThread;

    private final long publishIntervalMillis;
    /** The watermark last handed out for publication, or the resume point, which the task publishes when it starts. */
    private BytesRef lastPublished;
    private long lastPublishedMillis;
    private boolean publicationInFlight;

    /**
     * @param resumedFrom the {@code _tsid} the task resumes from, below which everything is already known to be in the target, or
     *                    {@code null} for a task starting from the beginning of the shard
     * @param publishIntervalMillis the minimum time between two publications of the watermark offered by
     *                              {@link #pollForPublication(long)}
     * @param nowMillis the current time, from the same clock {@link #pollForPublication(long)} is called with. The task publishes
     *                  its resume point when it starts, so the first periodic publication is due one interval after that.
     */
    ResumeWatermark(@Nullable BytesRef resumedFrom, long publishIntervalMillis, long nowMillis) {
        this.lastAdded = resumedFrom;
        this.publishIntervalMillis = publishIntervalMillis;
        this.lastPublished = resumedFrom;
        this.lastPublishedMillis = nowMillis;
    }

    /**
     * Records that a bucket was handed to the bulk processor tracks the first and last {@code _tsid}.
     *
     * @param tsid the {@code _tsid} being added to this bucket
     */
    void addedTsid(BytesRef tsid) {
        assert onScanThread() : "buckets must be added by the thread scanning the shard";
        assert tsid != null : "a bucket always has a _tsid";
        assert lastAdded == null || lastAdded.compareTo(tsid) <= 0 : "buckets must be added in _tsid order";
        if (firstOfBulkUnderConstruction == null) {
            firstOfBulkUnderConstruction = tsid;
        }
        lastAdded = tsid;
    }

    /**
     * Records a new bulk request in flight and associates it with the first {@code _tsid}.
     *
     * @param executionId the id of the bulk request.
     */
    void preparingBulkRequest(long executionId) {
        assert onScanThread() : "bulks must be sent by the thread scanning the shard";
        assert firstOfBulkUnderConstruction != null : "a bulk is only sent after a bucket was added";
        unconfirmedTsidsByRequestId.put(executionId, firstOfBulkUnderConstruction);
        firstOfBulkUnderConstruction = null;
    }

    /**
     * Records that a bulk succeeded, that is every one of its items was either indexed or rejected as already indexed.
     */
    void succeeded(long executionId) {
        unconfirmedTsidsByRequestId.remove(executionId);
    }

    /**
     * The {@code _tsid} to resume from, inclusive, or {@code null} if nothing is known to be indexed yet.
     */
    @Nullable
    BytesRef current() {
        assert onScanThread() : "the watermark is read by the thread scanning the shard";
        // A bulk confirmed after this snapshot only moves the safe point forward, so the result is at most outdated
        Map.Entry<Long, BytesRef> oldestUnconfirmed = unconfirmedTsidsByRequestId.firstEntry();
        if (oldestUnconfirmed != null) {
            return oldestUnconfirmed.getValue();
        }
        // Nothing sent is unconfirmed, and nothing but this thread can add to or send the bulk under construction
        return firstOfBulkUnderConstruction != null ? firstOfBulkUnderConstruction : lastAdded;
    }

    private boolean onScanThread() {
        if (scanThread == null) {
            scanThread = Thread.currentThread();
        }
        return scanThread == Thread.currentThread();
    }

    /**
     * The watermark to publish now, so that a task reassigned after its node is lost resumes from it, or {@code null} if there is
     * nothing to publish: the watermark has not moved since the last publication, a publication is still in flight, or the last
     * one is more recent than the publish interval. Every publication is a cluster state update, which is why they are throttled.
     * The caller reports the outcome with {@link #publicationSucceeded(BytesRef, long)} or {@link #publicationFailed(long)}.
     */
    @Nullable
    synchronized BytesRef pollForPublication(long nowMillis) {
        BytesRef watermark = current();
        if (publicationInFlight
            || watermark == null
            || watermark.equals(lastPublished)
            || nowMillis - lastPublishedMillis < publishIntervalMillis) {
            return null;
        }
        publicationInFlight = true;
        return watermark;
    }

    synchronized void publicationSucceeded(BytesRef published, long nowMillis) {
        assert publicationInFlight : "no publication in flight";
        publicationInFlight = false;
        lastPublished = published;
        lastPublishedMillis = nowMillis;
    }

    /**
     * The publication failed. The watermark is offered again once the publish interval has elapsed, rather than right away, so
     * that a master that keeps rejecting the update is not flooded.
     */
    synchronized void publicationFailed(long nowMillis) {
        assert publicationInFlight : "no publication in flight";
        publicationInFlight = false;
        lastPublishedMillis = nowMillis;
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.reindex.resumeinfo;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.BulkByScrollTask;
import org.elasticsearch.index.reindex.BulkByScrollTaskStatusTests;
import org.elasticsearch.index.reindex.ResumeInfo;
import org.elasticsearch.index.reindex.ResumeInfo.ScrollWorkerResumeInfo;
import org.elasticsearch.index.reindex.ResumeInfo.SliceStatus;
import org.elasticsearch.index.reindex.ResumeInfo.WorkerResult;
import org.elasticsearch.test.ESTestCase;

import java.util.Map;

import static java.util.Collections.emptyList;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

/**
 * Unit tests for {@link ResumeInfo} and its nested types. Serialization is covered by dedicated
 * wire serialization test suites.
 */
public class ResumeInfoTests extends ESTestCase {

    // ---------- ResumeInfo ----------

    /** Constructor rejects null worker with null slices, or with fewer than two slices. */
    public void testResumeInfoRequiresWorkerOrSlices() {
        expectThrows(IllegalArgumentException.class, () -> new ResumeInfo(null, null));
        expectThrows(
            IllegalArgumentException.class,
            () -> new ResumeInfo(null, Map.of(0, sliceStatusWithResult(0, new ElasticsearchException("e"))))
        );
    }

    /** Constructor rejects having both a worker and a slices map. */
    public void testResumeInfoCannotHaveBothWorkerAndSlices() {
        ScrollWorkerResumeInfo worker = scrollWorkerResumeInfo("sid", 1L, taskStatus());
        Map<Integer, SliceStatus> slices = Map.of(
            0,
            sliceStatusWithResult(0, new ElasticsearchException("e")),
            1,
            sliceStatusWithResult(1, new ElasticsearchException("e2"))
        );
        expectThrows(IllegalArgumentException.class, () -> new ResumeInfo(worker, slices));
    }

    public void testResumeInfoWithWorker() {
        ScrollWorkerResumeInfo worker = scrollWorkerResumeInfo("scroll-1", 100L, taskStatus());
        ResumeInfo info = new ResumeInfo(worker, null);
        assertThat(info.getTotalSlices(), equalTo(1));
        assertTrue(info.getWorker().isPresent());
        assertThat(info.getWorker().get(), equalTo(worker));
        assertTrue(info.getSlice(0).isEmpty());
        expectThrows(IllegalArgumentException.class, () -> info.isSliceCompleted(0));
    }

    public void testResumeInfoWithSlices() {
        SliceStatus s0 = sliceStatusWithResumeInfo(0, scrollWorkerResumeInfo("s0", 1L, taskStatus()));
        SliceStatus s1 = sliceStatusWithResult(1, new ElasticsearchException("fail"));
        Map<Integer, SliceStatus> slices = Map.of(0, s0, 1, s1);
        ResumeInfo info = new ResumeInfo(null, slices);
        assertThat(info.getTotalSlices(), equalTo(2));
        assertTrue(info.getWorker().isEmpty());
        assertTrue(info.getSlice(99).isEmpty());
        assertThat(info.getSlice(0).get(), equalTo(s0));
        assertThat(info.getSlice(1).get(), equalTo(s1));
        assertFalse(info.isSliceCompleted(0));
        assertTrue(info.isSliceCompleted(1));
        expectThrows(IllegalArgumentException.class, () -> info.isSliceCompleted(2));
    }

    /** Stored slices map is a copy; later mutations to the input map do not affect the instance. */
    public void testResumeInfoSlicesMapIsCopy() {
        Map<Integer, SliceStatus> mutable = new java.util.HashMap<>(
            Map.of(
                0,
                sliceStatusWithResult(0, new ElasticsearchException("a")),
                1,
                sliceStatusWithResult(1, new ElasticsearchException("b"))
            )
        );
        ResumeInfo info = new ResumeInfo(null, mutable);
        mutable.put(2, sliceStatusWithResult(2, new ElasticsearchException("c")));
        assertThat(info.slices().keySet(), hasSize(2));
    }

    // ---------- ScrollWorkerResumeInfo ----------

    /** Constructor rejects null scrollId. */
    public void testScrollWorkerResumeInfoRejectsNullScrollId() {
        expectThrows(NullPointerException.class, () -> new ScrollWorkerResumeInfo(null, 0L, taskStatus(), null));
    }

    /** Constructor rejects null status. */
    public void testScrollWorkerResumeInfoRejectsNullStatus() {
        expectThrows(NullPointerException.class, () -> new ScrollWorkerResumeInfo("sid", 0L, null, null));
    }

    public void testScrollWorkerResumeInfoAccessors() {
        String scrollId = "scroll-id";
        long startTime = 42L;
        BulkByScrollTask.Status status = taskStatus();
        Version remote = Version.CURRENT;
        ScrollWorkerResumeInfo info = new ScrollWorkerResumeInfo(scrollId, startTime, status, remote);
        assertThat(info.scrollId(), equalTo(scrollId));
        assertThat(info.startTimeEpochMillis(), equalTo(startTime));
        assertThat(info.status(), equalTo(status));
        assertThat(info.remoteVersion(), equalTo(remote));
        assertThat(info.getWriteableName(), equalTo(ScrollWorkerResumeInfo.NAME));
    }

    public void testScrollWorkerResumeInfoNullableRemoteVersion() {
        ScrollWorkerResumeInfo info = new ScrollWorkerResumeInfo("sid", 0L, taskStatus(), null);
        assertNull(info.remoteVersion());
    }

    // ---------- WorkerResult ----------

    /** Constructor rejects having both a response and a failure. */
    public void testWorkerResultRejectsBothResponseAndFailure() {
        BulkByScrollResponse response = new BulkByScrollResponse(TimeValue.ZERO, taskStatus(), emptyList(), emptyList(), false);
        expectThrows(IllegalArgumentException.class, () -> new WorkerResult(response, new ElasticsearchException("e")));
    }

    /** Constructor rejects having neither a response nor a failure. */
    public void testWorkerResultRequiresResponseOrFailure() {
        expectThrows(IllegalArgumentException.class, () -> new WorkerResult(null, null));
    }

    public void testWorkerResultWithResponse() {
        BulkByScrollResponse response = new BulkByScrollResponse(TimeValue.ZERO, taskStatus(), emptyList(), emptyList(), false);
        WorkerResult result = new WorkerResult(response, null);
        assertTrue(result.getResponse().isPresent());
        assertThat(result.getResponse().get(), equalTo(response));
        assertTrue(result.getFailure().isEmpty());
    }

    public void testWorkerResultWithFailure() {
        Exception e = new ElasticsearchException("msg");
        WorkerResult result = new WorkerResult(null, e);
        assertTrue(result.getResponse().isEmpty());
        assertTrue(result.getFailure().isPresent());
        assertThat(result.getFailure().get().getMessage(), equalTo("msg"));
    }

    // ---------- SliceStatus ----------

    /** Constructor rejects having both resumeInfo and result. */
    public void testSliceStatusRejectsBothResumeInfoAndResult() {
        ScrollWorkerResumeInfo resume = scrollWorkerResumeInfo("s", 0L, taskStatus());
        WorkerResult result = new WorkerResult(null, new ElasticsearchException("e"));
        expectThrows(IllegalArgumentException.class, () -> new SliceStatus(0, resume, result));
    }

    /** Constructor rejects having neither resumeInfo nor result. */
    public void testSliceStatusRequiresResumeInfoOrResult() {
        expectThrows(IllegalArgumentException.class, () -> new SliceStatus(0, null, null));
    }

    public void testSliceStatusWithResumeInfo() {
        ScrollWorkerResumeInfo resume = scrollWorkerResumeInfo("sid", 1L, taskStatus());
        SliceStatus status = new SliceStatus(5, resume, null);
        assertThat(status.sliceId(), equalTo(5));
        assertThat(status.resumeInfo(), equalTo(resume));
        assertNull(status.result());
        assertFalse(status.isCompleted());
    }

    public void testSliceStatusWithResultFailure() {
        WorkerResult result = new WorkerResult(null, new ElasticsearchException("err"));
        SliceStatus status = new SliceStatus(3, null, result);
        assertThat(status.sliceId(), equalTo(3));
        assertNull(status.resumeInfo());
        assertThat(status.result(), equalTo(result));
        assertTrue(status.isCompleted());
    }

    public void testSliceStatusWithResultResponse() {
        BulkByScrollResponse response = new BulkByScrollResponse(TimeValue.ZERO, taskStatus(), emptyList(), emptyList(), false);
        WorkerResult result = new WorkerResult(response, null);
        SliceStatus status = new SliceStatus(2, null, result);
        assertThat(status.sliceId(), equalTo(2));
        assertNull(status.resumeInfo());
        assertThat(status.result(), equalTo(result));
        assertTrue(status.isCompleted());
    }

    private static BulkByScrollTask.Status taskStatus() {
        return BulkByScrollTaskStatusTests.randomStatusWithoutException();
    }

    private static ScrollWorkerResumeInfo scrollWorkerResumeInfo(String scrollId, long startTime, BulkByScrollTask.Status status) {
        return new ScrollWorkerResumeInfo(scrollId, startTime, status, null);
    }

    private static SliceStatus sliceStatusWithResumeInfo(int sliceId, ScrollWorkerResumeInfo resumeInfo) {
        return new SliceStatus(sliceId, resumeInfo, null);
    }

    private static SliceStatus sliceStatusWithResult(int sliceId, Exception failure) {
        return new SliceStatus(sliceId, null, new WorkerResult(null, failure));
    }
}

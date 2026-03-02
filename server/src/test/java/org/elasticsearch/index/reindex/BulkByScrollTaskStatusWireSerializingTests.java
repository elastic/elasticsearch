/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.reindex;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static java.lang.Math.abs;

/**
 * Wire serialization tests for {@link BulkByScrollTask.Status}.
 * Uses a wrapper type so that equality after round-trip is semantic (e.g. exception messages
 * instead of instance identity), matching {@link BulkByScrollTaskStatusTests#assertTaskStatusEquals}.
 */
public class BulkByScrollTaskStatusWireSerializingTests extends AbstractWireSerializingTestCase<
    BulkByScrollTaskStatusWireSerializingTests.StatusWrapper> {

    @Override
    protected Writeable.Reader<StatusWrapper> instanceReader() {
        return StatusWrapper::new;
    }

    @Override
    protected StatusWrapper createTestInstance() {
        return new StatusWrapper(BulkByScrollTaskStatusTests.randomStatus());
    }

    @Override
    protected StatusWrapper mutateInstance(StatusWrapper instance) throws IOException {
        return new StatusWrapper(mutateStatus(instance.status));
    }

    /**
     * Returns a copy of the given status with exactly one field changed. Used by
     * {@link BulkByScrollTaskStatusOrExceptionWireSerializingTests} when mutating the status field.
     */
    public static BulkByScrollTask.Status mutateStatus(BulkByScrollTask.Status status) {
        if (status.getSliceStatuses().isEmpty()) {
            return mutateSingleFieldStatus(status);
        } else {
            return mutateMergedStatus(status);
        }
    }

    private static BulkByScrollTask.Status mutateSingleFieldStatus(BulkByScrollTask.Status status) {
        int field = between(0, 13);
        Integer sliceId = status.getSliceId();
        long total = status.getTotal();
        long updated = status.getUpdated();
        long created = status.getCreated();
        long deleted = status.getDeleted();
        int batches = status.getBatches();
        long versionConflicts = status.getVersionConflicts();
        long noops = status.getNoops();
        long bulkRetries = status.getBulkRetries();
        long searchRetries = status.getSearchRetries();
        TimeValue throttled = status.getThrottled();
        float requestsPerSecond = status.getRequestsPerSecond();
        String reasonCancelled = status.getReasonCancelled();
        TimeValue throttledUntil = status.getThrottledUntil();

        switch (field) {
            case 0 -> sliceId = randomValueOtherThan(sliceId, () -> randomBoolean() ? null : between(1, 100));
            case 1 -> total = randomValueOtherThan(total, () -> randomLongBetween(0, 100));
            case 2 -> updated = randomValueOtherThan(updated, () -> randomLongBetween(0, 100));
            case 3 -> created = randomValueOtherThan(created, () -> randomLongBetween(0, 100));
            case 4 -> deleted = randomValueOtherThan(deleted, () -> randomLongBetween(0, 100));
            case 5 -> batches = randomValueOtherThan(batches, () -> between(0, 100));
            case 6 -> versionConflicts = randomValueOtherThan(versionConflicts, () -> randomLongBetween(0, 100));
            case 7 -> noops = randomValueOtherThan(noops, () -> randomLongBetween(0, 100));
            case 8 -> bulkRetries = randomValueOtherThan(bulkRetries, () -> randomLongBetween(0, 100));
            case 9 -> searchRetries = randomValueOtherThan(searchRetries, () -> randomLongBetween(0, 100));
            case 10 -> throttled = randomValueOtherThan(throttled, BulkByScrollTaskStatusWireSerializingTests::randomTimeValue);
            case 11 -> requestsPerSecond = randomValueOtherThan(status.getRequestsPerSecond(), () -> abs(Randomness.get().nextFloat()));
            case 12 -> reasonCancelled = randomValueOtherThan(reasonCancelled, () -> randomBoolean() ? null : randomAlphaOfLength(10));
            default -> throttledUntil = randomValueOtherThan(throttledUntil, BulkByScrollTaskStatusWireSerializingTests::randomTimeValue);
        }

        return new BulkByScrollTask.Status(
            sliceId,
            total,
            updated,
            created,
            deleted,
            batches,
            versionConflicts,
            noops,
            bulkRetries,
            searchRetries,
            throttled,
            requestsPerSecond,
            reasonCancelled,
            throttledUntil
        );
    }

    private static BulkByScrollTask.Status mutateMergedStatus(BulkByScrollTask.Status status) {
        int field = between(0, 1);
        if (field == 0) {
            String reasonCancelled = randomValueOtherThan(
                status.getReasonCancelled(),
                () -> randomBoolean() ? null : randomAlphaOfLength(10)
            );
            return new BulkByScrollTask.Status(status.getSliceStatuses(), reasonCancelled);
        } else {
            List<BulkByScrollTask.StatusOrException> newSlices = new ArrayList<>(status.getSliceStatuses());
            newSlices.add(randomSliceStatusOrException());
            return new BulkByScrollTask.Status(newSlices, status.getReasonCancelled());
        }
    }

    private static BulkByScrollTask.StatusOrException randomSliceStatusOrException() {
        return switch (between(0, 2)) {
            case 0 -> null;
            case 1 -> new BulkByScrollTask.StatusOrException(new ElasticsearchException(randomAlphaOfLength(5)));
            default -> new BulkByScrollTask.StatusOrException(BulkByScrollTaskStatusTests.randomWorkingStatus(between(0, 100)));
        };
    }

    @Override
    protected void assertEqualInstances(StatusWrapper expectedInstance, StatusWrapper newInstance) {
        assertNotSame(expectedInstance, newInstance);
        BulkByScrollTaskStatusTests.assertTaskStatusEquals(expectedInstance.status, newInstance.status);
    }

    /**
     * Wrapper around {@link BulkByScrollTask.Status} that implements semantic equality and hashCode
     * so that round-trip serialization passes (e.g. exceptions in slice statuses are compared by
     * message, not reference).
     */
    static class StatusWrapper implements Writeable {
        private final BulkByScrollTask.Status status;

        StatusWrapper(BulkByScrollTask.Status status) {
            this.status = status;
        }

        StatusWrapper(StreamInput in) throws IOException {
            this.status = new BulkByScrollTask.Status(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            status.writeTo(out);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            StatusWrapper that = (StatusWrapper) o;
            return statusEquals(status, that.status);
        }

        @Override
        public int hashCode() {
            return statusHashCode(status);
        }

        static boolean statusEquals(BulkByScrollTask.Status a, BulkByScrollTask.Status b) {
            if (a == b) return true;
            if (a == null || b == null) return false;
            if (a.getTotal() != b.getTotal()) return false;
            if (a.getUpdated() != b.getUpdated()) return false;
            if (a.getCreated() != b.getCreated()) return false;
            if (a.getDeleted() != b.getDeleted()) return false;
            if (a.getBatches() != b.getBatches()) return false;
            if (a.getVersionConflicts() != b.getVersionConflicts()) return false;
            if (a.getNoops() != b.getNoops()) return false;
            if (a.getBulkRetries() != b.getBulkRetries()) return false;
            if (a.getSearchRetries() != b.getSearchRetries()) return false;
            if (Float.compare(a.getRequestsPerSecond(), b.getRequestsPerSecond()) != 0) return false;
            if (Objects.equals(a.getSliceId(), b.getSliceId()) == false) return false;
            if (Objects.equals(a.getThrottled(), b.getThrottled()) == false) return false;
            if (Objects.equals(a.getReasonCancelled(), b.getReasonCancelled()) == false) return false;
            if (Objects.equals(a.getThrottledUntil(), b.getThrottledUntil()) == false) return false;
            List<BulkByScrollTask.StatusOrException> sa = a.getSliceStatuses();
            List<BulkByScrollTask.StatusOrException> sb = b.getSliceStatuses();
            if (sa.size() != sb.size()) return false;
            for (int i = 0; i < sa.size(); i++) {
                if (statusOrExceptionEquals(sa.get(i), sb.get(i)) == false) return false;
            }
            return true;
        }

        private static boolean statusOrExceptionEquals(BulkByScrollTask.StatusOrException a, BulkByScrollTask.StatusOrException b) {
            if (a == b) return true;
            if (a == null || b == null) return false;
            if (a.getStatus() != null && b.getStatus() != null) return statusEquals(a.getStatus(), b.getStatus());
            if (a.getException() != null && b.getException() != null) {
                return Objects.equals(a.getException().getMessage(), b.getException().getMessage());
            }
            return false;
        }

        static int statusHashCode(BulkByScrollTask.Status s) {
            if (s == null) return 0;
            int h = Objects.hash(
                s.getSliceId(),
                s.getTotal(),
                s.getUpdated(),
                s.getCreated(),
                s.getDeleted(),
                s.getBatches(),
                s.getVersionConflicts(),
                s.getNoops(),
                s.getBulkRetries(),
                s.getSearchRetries(),
                s.getThrottled(),
                s.getRequestsPerSecond(),
                s.getReasonCancelled(),
                s.getThrottledUntil()
            );
            for (BulkByScrollTask.StatusOrException soe : s.getSliceStatuses()) {
                h = 31 * h + statusOrExceptionHashCode(soe);
            }
            return h;
        }

        private static int statusOrExceptionHashCode(BulkByScrollTask.StatusOrException statusOrException) {
            if (statusOrException == null) return 0;
            if (statusOrException.getStatus() != null) return statusHashCode(statusOrException.getStatus());
            if (statusOrException.getException() != null) return Objects.hashCode(statusOrException.getException().getMessage());
            return 0;
        }
    }
}

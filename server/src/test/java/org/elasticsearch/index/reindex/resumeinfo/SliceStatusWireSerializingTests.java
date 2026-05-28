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
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.IndexDocFailureStoreStatus;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.reindex.BulkByPaginatedSearchTask;
import org.elasticsearch.index.reindex.BulkByPaginatedSearchTaskStatusTests;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.PaginatedSearchFailure;
import org.elasticsearch.index.reindex.ResumeInfo.PitWorkerResumeInfo;
import org.elasticsearch.index.reindex.ResumeInfo.ScrollWorkerResumeInfo;
import org.elasticsearch.index.reindex.ResumeInfo.SliceStatus;
import org.elasticsearch.index.reindex.ResumeInfo.WorkerResult;
import org.elasticsearch.index.reindex.ResumeInfo.WorkerResumeInfo;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.index.reindex.resumeinfo.PitWorkerResumeInfoWireSerializingTests.pitWorkerResumeInfoContentEquals;
import static org.elasticsearch.index.reindex.resumeinfo.PitWorkerResumeInfoWireSerializingTests.pitWorkerResumeInfoContentHashCode;
import static org.elasticsearch.index.reindex.resumeinfo.PitWorkerResumeInfoWireSerializingTests.randomPitWorkerResumeInfo;
import static org.elasticsearch.index.reindex.resumeinfo.ScrollWorkerResumeInfoWireSerializingTests.randomScrollWorkerResumeInfo;

/**
 * Wire serialization tests for {@link SliceStatus}.
 * Uses a {@link Wrapper} with content-based equals/hashCode because {@link SliceStatus}
 * embeds {@link BulkByScrollResponse} and {@link Exception} without structural {@code equals}.
 */
public class SliceStatusWireSerializingTests extends AbstractWireSerializingTestCase<SliceStatusWireSerializingTests.Wrapper> {

    /**
     * Register {@link WorkerResumeInfo} and {@link Task.Status} so that
     * {@link SliceStatus}'s optional resumeInfo and nested status can be deserialized.
     */
    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        List<NamedWriteableRegistry.Entry> entries = new ArrayList<>(org.elasticsearch.cluster.ClusterModule.getNamedWriteables());
        entries.add(
            new NamedWriteableRegistry.Entry(
                Task.Status.class,
                BulkByPaginatedSearchTask.Status.NAME,
                BulkByPaginatedSearchTask.Status::new
            )
        );
        entries.add(new NamedWriteableRegistry.Entry(WorkerResumeInfo.class, ScrollWorkerResumeInfo.NAME, ScrollWorkerResumeInfo::new));
        entries.add(new NamedWriteableRegistry.Entry(WorkerResumeInfo.class, PitWorkerResumeInfo.NAME, PitWorkerResumeInfo::new));
        return new NamedWriteableRegistry(entries);
    }

    @Override
    protected Writeable.Reader<Wrapper> instanceReader() {
        return Wrapper::new;
    }

    @Override
    protected Wrapper createTestInstance() {
        return new Wrapper(randomSliceStatusWithId(randomIntBetween(0, 100)));
    }

    @Override
    protected Wrapper mutateInstance(Wrapper instance) throws IOException {
        // SliceStatus is a record; no need to verify equality via mutations
        return null;
    }

    public static boolean workerResumeInfoContentEquals(WorkerResumeInfo a, WorkerResumeInfo b) {
        if (Objects.equals(a, b)) return true;
        if (a == null || b == null) return false;
        if (a instanceof PitWorkerResumeInfo pa && b instanceof PitWorkerResumeInfo pb) {
            return pitWorkerResumeInfoContentEquals(pa, pb);
        }
        return a.equals(b);
    }

    public static int workerResumeInfoContentHashCode(WorkerResumeInfo info) {
        if (info == null) return 0;
        if (info instanceof PitWorkerResumeInfo pit) {
            return pitWorkerResumeInfoContentHashCode(pit);
        }
        return info.hashCode();
    }

    public static boolean sliceStatusContentEquals(SliceStatus a, SliceStatus b) {
        if (a.sliceId() != b.sliceId()) {
            return false;
        }
        if (workerResumeInfoContentEquals(a.resumeInfo(), b.resumeInfo()) == false) {
            return false;
        }
        if (a.result() == null && b.result() == null) return true;
        if (a.result() == null || b.result() == null) {
            return false;
        }
        return workerResultContentEquals(a.result(), b.result());
    }

    public static int sliceStatusContentHashCode(SliceStatus status) {
        int result = Integer.hashCode(status.sliceId());
        result = 31 * result + workerResumeInfoContentHashCode(status.resumeInfo());
        if (status.result() != null) {
            if (status.result().getResponse().isPresent()) {
                result = 31 * result + bulkByScrollResponseContentHashCode(status.result().getResponse().get());
            } else {
                result = 31 * result + Objects.hashCode(status.result().getFailure().get().getMessage());
            }
        }
        return result;
    }

    static boolean workerResultContentEquals(WorkerResult a, WorkerResult b) {
        if (a.getResponse().isPresent()) {
            if (b.getResponse().isPresent() == false) {
                return false;
            }
            return bulkByScrollResponseContentEquals(a.getResponse().get(), b.getResponse().get());
        } else {
            if (b.getFailure().isPresent() == false) {
                return false;
            }
            return Objects.equals(a.getFailure().get().getMessage(), b.getFailure().get().getMessage());
        }
    }

    static boolean bulkByScrollResponseContentEquals(BulkByScrollResponse a, BulkByScrollResponse b) {
        if (Objects.equals(a.getTook(), b.getTook()) == false) {
            return false;
        }
        if (a.getStatus().equals(b.getStatus()) == false) {
            return false;
        }
        if (a.isTimedOut() != b.isTimedOut()) {
            return false;
        }
        if (bulkFailuresContentEquals(a.getBulkFailures(), b.getBulkFailures()) == false) {
            return false;
        }
        return searchFailuresContentEquals(a.getSearchFailures(), b.getSearchFailures());
    }

    static int bulkByScrollResponseContentHashCode(BulkByScrollResponse response) {
        int result = Objects.hash(response.getTook(), response.getStatus(), response.isTimedOut());
        result = 31 * result + bulkFailuresContentHashCode(response.getBulkFailures());
        result = 31 * result + searchFailuresContentHashCode(response.getSearchFailures());
        return result;
    }

    private static boolean bulkFailuresContentEquals(List<BulkItemResponse.Failure> a, List<BulkItemResponse.Failure> b) {
        if (a.size() != b.size()) {
            return false;
        }
        for (int i = 0; i < a.size(); i++) {
            if (bulkFailureContentEquals(a.get(i), b.get(i)) == false) {
                return false;
            }
        }
        return true;
    }

    private static int bulkFailuresContentHashCode(List<BulkItemResponse.Failure> failures) {
        int result = 1;
        for (BulkItemResponse.Failure f : failures) {
            result = 31 * result + bulkFailureContentHashCode(f);
        }
        return result;
    }

    private static boolean bulkFailureContentEquals(BulkItemResponse.Failure a, BulkItemResponse.Failure b) {
        return Objects.equals(a.getIndex(), b.getIndex())
            && Objects.equals(a.getId(), b.getId())
            && a.getStatus() == b.getStatus()
            && a.getSeqNo() == b.getSeqNo()
            && a.getTerm() == b.getTerm()
            && a.isAborted() == b.isAborted()
            && a.getFailureStoreStatus() == b.getFailureStoreStatus()
            && throwablesShallowEquals(a.getCause(), b.getCause());
    }

    private static int bulkFailureContentHashCode(BulkItemResponse.Failure f) {
        return Objects.hash(
            f.getIndex(),
            f.getId(),
            f.getStatus(),
            f.getSeqNo(),
            f.getTerm(),
            f.isAborted(),
            f.getFailureStoreStatus(),
            f.getCause() == null ? null : f.getCause().getClass().getName(),
            f.getCause() == null ? null : f.getCause().getMessage()
        );
    }

    private static boolean searchFailuresContentEquals(List<PaginatedSearchFailure> a, List<PaginatedSearchFailure> b) {
        if (a.size() != b.size()) {
            return false;
        }
        for (int i = 0; i < a.size(); i++) {
            if (paginatedSearchFailureContentEquals(a.get(i), b.get(i)) == false) {
                return false;
            }
        }
        return true;
    }

    private static int searchFailuresContentHashCode(List<PaginatedSearchFailure> failures) {
        int result = 1;
        for (PaginatedSearchFailure f : failures) {
            result = 31 * result + paginatedSearchFailureContentHashCode(f);
        }
        return result;
    }

    private static boolean paginatedSearchFailureContentEquals(PaginatedSearchFailure a, PaginatedSearchFailure b) {
        return a.getStatus() == b.getStatus()
            && Objects.equals(a.getIndex(), b.getIndex())
            && Objects.equals(a.getShardId(), b.getShardId())
            && Objects.equals(a.getNodeId(), b.getNodeId())
            && throwablesShallowEquals(a.getReason(), b.getReason());
    }

    private static int paginatedSearchFailureContentHashCode(PaginatedSearchFailure f) {
        Throwable reason = f.getReason();
        return Objects.hash(
            f.getStatus(),
            f.getIndex(),
            f.getShardId(),
            f.getNodeId(),
            reason == null ? null : reason.getClass().getName(),
            reason == null ? null : reason.getMessage()
        );
    }

    private static boolean throwablesShallowEquals(Throwable a, Throwable b) {
        if (a == b) return true;
        if (a == null || b == null) return false;
        return a.getClass().equals(b.getClass()) && Objects.equals(a.getMessage(), b.getMessage());
    }

    /**
     * Compares slice maps (e.g. {@link org.elasticsearch.index.reindex.ResumeInfo#slices()}) for wire-test equality.
     */
    public static boolean sliceMapsContentEqual(Map<Integer, SliceStatus> first, Map<Integer, SliceStatus> second) {
        if (first.size() != second.size()) {
            return false;
        }
        for (Map.Entry<Integer, SliceStatus> entry : first.entrySet()) {
            SliceStatus otherSlice = second.get(entry.getKey());
            if (otherSlice == null || sliceStatusContentEquals(entry.getValue(), otherSlice) == false) {
                return false;
            }
        }
        return true;
    }

    /**
     * Hash code consistent with {@link #sliceMapsContentEqual(Map, Map)} for wire-test wrappers.
     */
    public static int sliceMapContentHashCode(Map<Integer, SliceStatus> slices) {
        List<Integer> keys = new ArrayList<>(slices.keySet());
        Collections.sort(keys);
        int result = 1;
        for (Integer key : keys) {
            result = 31 * result + key;
            result = 31 * result + sliceStatusContentHashCode(slices.get(key));
        }
        return result;
    }

    /**
     * Wrapper around {@link SliceStatus} that implements content-based equals/hashCode so that
     * round-trip serialization tests pass when the slice contains {@link BulkByScrollResponse} or {@link Exception}.
     */
    public static final class Wrapper implements Writeable {
        private final SliceStatus delegate;

        public Wrapper(SliceStatus delegate) {
            this.delegate = delegate;
        }

        public Wrapper(StreamInput in) throws IOException {
            this(new SliceStatus(in));
        }

        public SliceStatus delegate() {
            return delegate;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            delegate.writeTo(out);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null || getClass() != obj.getClass()) return false;
            Wrapper other = (Wrapper) obj;
            return sliceStatusContentEquals(this.delegate, other.delegate);
        }

        @Override
        public int hashCode() {
            return sliceStatusContentHashCode(delegate);
        }
    }

    public static SliceStatus randomSliceStatusWithId(int sliceId) {
        if (randomBoolean()) {
            WorkerResumeInfo resumeInfo = randomBoolean() ? randomScrollWorkerResumeInfo() : randomPitWorkerResumeInfo();
            return new SliceStatus(sliceId, resumeInfo, null);
        } else {
            return new SliceStatus(
                sliceId,
                null,
                randomBoolean()
                    ? new WorkerResult(randomBulkByScrollResponse(), null)
                    : new WorkerResult(null, new ElasticsearchException(randomAlphaOfLength(5)))
            );
        }
    }

    static BulkByScrollResponse randomBulkByScrollResponse() {
        return new BulkByScrollResponse(
            TimeValue.timeValueMillis(randomNonNegativeLong()),
            BulkByPaginatedSearchTaskStatusTests.randomStatusWithoutException(),
            randomBulkFailuresList(),
            randomSearchFailuresList(),
            randomBoolean()
        );
    }

    private static List<BulkItemResponse.Failure> randomBulkFailuresList() {
        int n = randomIntBetween(0, 2);
        List<BulkItemResponse.Failure> list = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            list.add(randomBulkFailure());
        }
        return list;
    }

    private static BulkItemResponse.Failure randomBulkFailure() {
        return new BulkItemResponse.Failure(
            randomAlphaOfLengthBetween(1, 10),
            randomBoolean() ? randomAlphaOfLengthBetween(1, 10) : null,
            new ElasticsearchException(randomAlphaOfLengthBetween(1, 20)),
            randomLong(),
            randomNonNegativeLong(),
            randomBoolean(),
            randomFrom(IndexDocFailureStoreStatus.values())
        );
    }

    private static List<PaginatedSearchFailure> randomSearchFailuresList() {
        int n = randomIntBetween(0, 2);
        List<PaginatedSearchFailure> list = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            list.add(randomSearchFailure());
        }
        return list;
    }

    private static PaginatedSearchFailure randomSearchFailure() {
        return new PaginatedSearchFailure(
            new ElasticsearchException(randomAlphaOfLengthBetween(1, 20)),
            randomBoolean() ? randomAlphaOfLengthBetween(1, 10) : null,
            randomBoolean() ? randomIntBetween(0, 10) : null,
            randomBoolean() ? randomAlphaOfLengthBetween(1, 10) : null
        );
    }
}

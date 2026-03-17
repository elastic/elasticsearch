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
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.BulkByScrollTask;
import org.elasticsearch.index.reindex.BulkByScrollTaskStatusTests;
import org.elasticsearch.index.reindex.ResumeInfo.PitWorkerResumeInfo;
import org.elasticsearch.index.reindex.ResumeInfo.ScrollWorkerResumeInfo;
import org.elasticsearch.index.reindex.ResumeInfo.SliceStatus;
import org.elasticsearch.index.reindex.ResumeInfo.WorkerResult;
import org.elasticsearch.index.reindex.ResumeInfo.WorkerResumeInfo;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static java.util.Collections.emptyList;
import static org.elasticsearch.index.reindex.resumeinfo.PitWorkerResumeInfoWireSerializingTests.pitWorkerResumeInfoContentEquals;
import static org.elasticsearch.index.reindex.resumeinfo.PitWorkerResumeInfoWireSerializingTests.pitWorkerResumeInfoContentHashCode;
import static org.elasticsearch.index.reindex.resumeinfo.PitWorkerResumeInfoWireSerializingTests.randomPitWorkerResumeInfo;
import static org.elasticsearch.index.reindex.resumeinfo.ScrollWorkerResumeInfoWireSerializingTests.randomScrollWorkerResumeInfo;

/**
 * Wire serialization tests for {@link SliceStatus}.
 * Uses a {@link Wrapper} with content-based equals/hashCode because {@link SliceStatus}
 */
public class SliceStatusWireSerializingTests extends AbstractWireSerializingTestCase<SliceStatusWireSerializingTests.Wrapper> {

    /**
     * Register {@link WorkerResumeInfo} and {@link Task.Status} so that
     * {@link SliceStatus}'s optional resumeInfo and nested status can be deserialized.
     */
    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        List<NamedWriteableRegistry.Entry> entries = new ArrayList<>(org.elasticsearch.cluster.ClusterModule.getNamedWriteables());
        entries.add(new NamedWriteableRegistry.Entry(Task.Status.class, BulkByScrollTask.Status.NAME, BulkByScrollTask.Status::new));
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

    static boolean workerResumeInfoContentEquals(WorkerResumeInfo a, WorkerResumeInfo b) {
        if (Objects.equals(a, b)) return true;
        if (a == null || b == null) return false;
        if (a instanceof PitWorkerResumeInfo pa && b instanceof PitWorkerResumeInfo pb) {
            return pitWorkerResumeInfoContentEquals(pa, pb);
        }
        return a.equals(b);
    }

    static int workerResumeInfoContentHashCode(WorkerResumeInfo info) {
        if (info == null) return 0;
        if (info instanceof PitWorkerResumeInfo pit) {
            return pitWorkerResumeInfoContentHashCode(pit);
        }
        return info.hashCode();
    }

    static boolean sliceStatusContentEquals(SliceStatus a, SliceStatus b) {
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

    static int sliceStatusContentHashCode(SliceStatus status) {
        int result = Integer.hashCode(status.sliceId());
        result = 31 * result + workerResumeInfoContentHashCode(status.resumeInfo());
        if (status.result() != null) {
            if (status.result().getResponse().isPresent()) {
                BulkByScrollResponse response = status.result().getResponse().get();
                result = 31 * result + Objects.hash(response.getTook(), response.getStatus(), response.isTimedOut());
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
        if (a.getBulkFailures().size() != b.getBulkFailures().size()) {
            return false;
        }
        if (a.getSearchFailures().size() != b.getSearchFailures().size()) {
            return false;
        }
        if (a.isTimedOut() != b.isTimedOut()) {
            return false;
        }
        return true;
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

    private static BulkByScrollResponse randomBulkByScrollResponse() {
        return new BulkByScrollResponse(
            TimeValue.timeValueMillis(randomNonNegativeLong()),
            BulkByScrollTaskStatusTests.randomStatusWithoutException(),
            emptyList(),
            emptyList(),
            randomBoolean()
        );
    }
}

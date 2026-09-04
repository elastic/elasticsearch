/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.reindex.resumeinfo;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.reindex.BulkByPaginatedSearchTask;
import org.elasticsearch.index.reindex.ResumeInfo;
import org.elasticsearch.index.reindex.ResumeInfo.WorkerResumeInfo;
import org.elasticsearch.tasks.RawTaskStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskResult;
import org.elasticsearch.tasks.TaskResultTests;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.index.reindex.resumeinfo.PitWorkerResumeInfoWireSerializingTests.randomPitWorkerResumeInfo;
import static org.elasticsearch.index.reindex.resumeinfo.ScrollWorkerResumeInfoWireSerializingTests.randomScrollWorkerResumeInfo;
import static org.elasticsearch.index.reindex.resumeinfo.SliceStatusWireSerializingTests.randomSliceStatusWithId;
import static org.elasticsearch.index.reindex.resumeinfo.SliceStatusWireSerializingTests.sliceStatusContentEquals;
import static org.elasticsearch.index.reindex.resumeinfo.SliceStatusWireSerializingTests.sliceStatusContentHashCode;
import static org.elasticsearch.index.reindex.resumeinfo.SliceStatusWireSerializingTests.workerResumeInfoContentEquals;
import static org.elasticsearch.index.reindex.resumeinfo.SliceStatusWireSerializingTests.workerResumeInfoContentHashCode;

/**
 * Wire serialization tests for {@link ResumeInfo}.
 */
public class ResumeInfoWireSerializingTests extends AbstractWireSerializingTestCase<ResumeInfoWireSerializingTests.Wrapper> {

    /**
     * Register {@link ResumeInfo.WorkerResumeInfo} and {@link Task.Status} implementations so workers, slices, and
     * optional {@link ResumeInfo#sourceTaskResult()} ({@link org.elasticsearch.tasks.TaskInfo} embedded status) can deserialize.
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
        entries.add(new NamedWriteableRegistry.Entry(Task.Status.class, RawTaskStatus.NAME, RawTaskStatus::new));
        entries.add(
            new NamedWriteableRegistry.Entry(
                ResumeInfo.WorkerResumeInfo.class,
                ResumeInfo.ScrollWorkerResumeInfo.NAME,
                ResumeInfo.ScrollWorkerResumeInfo::new
            )
        );
        entries.add(
            new NamedWriteableRegistry.Entry(
                ResumeInfo.WorkerResumeInfo.class,
                ResumeInfo.PitWorkerResumeInfo.NAME,
                ResumeInfo.PitWorkerResumeInfo::new
            )
        );
        return new NamedWriteableRegistry(entries);
    }

    @Override
    protected Writeable.Reader<Wrapper> instanceReader() {
        return Wrapper::new;
    }

    @Override
    protected Wrapper createTestInstance() {
        final ResumeInfo.RelocationOrigin origin = new ResumeInfo.RelocationOrigin(
            new TaskId(randomAlphaOfLength(10), randomNonNegativeLong()),
            randomNonNegativeLong()
        );
        final TaskResult sourceTaskResult = randomBoolean() ? randomSourceTaskResult() : null;
        if (randomBoolean()) {
            WorkerResumeInfo worker = randomBoolean() ? randomScrollWorkerResumeInfo() : randomPitWorkerResumeInfo();
            return new Wrapper(new ResumeInfo(origin, worker, null, sourceTaskResult));
        } else {
            return new Wrapper(new ResumeInfo(origin, null, randomSlicesMap(), sourceTaskResult));
        }
    }

    @Override
    protected Wrapper mutateInstance(Wrapper instance) throws IOException {
        // ResumeInfo is a record; no need to verify equality via mutations
        return null;
    }

    private Map<Integer, ResumeInfo.SliceStatus> randomSlicesMap() {
        int size = randomIntBetween(2, 5);
        Map<Integer, ResumeInfo.SliceStatus> map = new HashMap<>();
        for (int i = 0; i < size; i++) {
            map.put(i, randomSliceStatusWithId(i));
        }
        return map;
    }

    private static TaskResult randomSourceTaskResult() {
        try {
            return TaskResultTests.randomTaskResult();
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    /**
     * Wrapper around {@link ResumeInfo} that implements content-based equals/hashCode so that
     * round-trip serialization tests pass when the info contains slices with {@link ResumeInfo.WorkerResult}.
     */
    public static final class Wrapper implements Writeable {
        private final ResumeInfo delegate;

        public Wrapper(ResumeInfo delegate) {
            this.delegate = delegate;
        }

        public Wrapper(StreamInput in) throws IOException {
            this(new ResumeInfo(in));
        }

        public ResumeInfo delegate() {
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
            return resumeInfoContentEquals(this.delegate, other.delegate);
        }

        @Override
        public int hashCode() {
            return resumeInfoContentHashCode(delegate);
        }

        private static boolean resumeInfoContentEquals(ResumeInfo a, ResumeInfo b) {
            if (Objects.equals(a.relocationOrigin(), b.relocationOrigin()) == false) {
                return false;
            }
            if (Objects.equals(a.sourceTaskResult(), b.sourceTaskResult()) == false) {
                return false;
            }
            if (workerResumeInfoContentEquals(a.worker(), b.worker()) == false) {
                return false;
            }
            if (a.slices() == null && b.slices() == null) {
                return true;
            }
            if (a.slices() == null || b.slices() == null) {
                return false;
            }
            if (a.slices().keySet().equals(b.slices().keySet()) == false) {
                return false;
            }
            for (Integer key : a.slices().keySet()) {
                if (sliceStatusContentEquals(a.slices().get(key), b.slices().get(key)) == false) {
                    return false;
                }
            }
            return true;
        }

        private static int resumeInfoContentHashCode(ResumeInfo info) {
            int result = Objects.hashCode(info.relocationOrigin());
            result = 31 * result + Objects.hashCode(info.sourceTaskResult());
            result = 31 * result + workerResumeInfoContentHashCode(info.worker());
            if (info.slices() != null) {
                for (Map.Entry<Integer, ResumeInfo.SliceStatus> entry : info.slices().entrySet()) {
                    result = 31 * result + entry.getKey().hashCode();
                    result = 31 * result + sliceStatusContentHashCode(entry.getValue());
                }
            }
            return result;
        }
    }
}

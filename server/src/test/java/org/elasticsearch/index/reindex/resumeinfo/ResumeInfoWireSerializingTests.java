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
import org.elasticsearch.index.reindex.BulkByScrollTask;
import org.elasticsearch.index.reindex.ResumeInfo;
import org.elasticsearch.index.reindex.ResumeInfo.WorkerResumeInfo;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.index.reindex.resumeinfo.PitWorkerResumeInfoWireSerializingTests.randomPitWorkerResumeInfo;
import static org.elasticsearch.index.reindex.resumeinfo.ScrollWorkerResumeInfoWireSerializingTests.randomScrollWorkerResumeInfo;
import static org.elasticsearch.index.reindex.resumeinfo.SliceStatusWireSerializingTests.randomSliceStatusWithId;
import static org.elasticsearch.index.reindex.resumeinfo.SliceStatusWireSerializingTests.sliceStatusContentEquals;
import static org.elasticsearch.index.reindex.resumeinfo.SliceStatusWireSerializingTests.sliceStatusContentHashCode;
import static org.elasticsearch.index.reindex.resumeinfo.SliceStatusWireSerializingTests.workerResumeInfoContentEquals;
import static org.elasticsearch.index.reindex.resumeinfo.SliceStatusWireSerializingTests.workerResumeInfoContentHashCode;

/**
 * Wire serialization tests for {@link ResumeInfo}.
 * Uses a {@link Wrapper} with content-based equals/hashCode because {@link ResumeInfo} can contain
 */
public class ResumeInfoWireSerializingTests extends AbstractWireSerializingTestCase<ResumeInfoWireSerializingTests.Wrapper> {

    /**
     * Register {@link ResumeInfo.WorkerResumeInfo} and {@link Task.Status} so that the optional worker
     * and any nested types inside slices can be deserialized when round-tripping {@link ResumeInfo}.
     */
    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        List<NamedWriteableRegistry.Entry> entries = new ArrayList<>(org.elasticsearch.cluster.ClusterModule.getNamedWriteables());
        entries.add(new NamedWriteableRegistry.Entry(Task.Status.class, BulkByScrollTask.Status.NAME, BulkByScrollTask.Status::new));
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
        if (randomBoolean()) {
            WorkerResumeInfo worker = randomBoolean() ? randomScrollWorkerResumeInfo() : randomPitWorkerResumeInfo();
            return new Wrapper(new ResumeInfo(origin, worker, null));
        } else {
            return new Wrapper(new ResumeInfo(origin, null, randomSlicesMap()));
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
            if (workerResumeInfoContentEquals(a.worker(), b.worker()) == false) return false;
            if (a.slices() == null && b.slices() == null) return true;
            if (a.slices() == null || b.slices() == null) return false;
            if (a.slices().keySet().equals(b.slices().keySet()) == false) return false;
            for (Integer key : a.slices().keySet()) {
                if (sliceStatusContentEquals(a.slices().get(key), b.slices().get(key)) == false) return false;
            }
            return true;
        }

        private static int resumeInfoContentHashCode(ResumeInfo info) {
            int result = workerResumeInfoContentHashCode(info.worker());
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

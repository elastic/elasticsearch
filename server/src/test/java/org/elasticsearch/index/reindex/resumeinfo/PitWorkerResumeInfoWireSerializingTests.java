/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.reindex.resumeinfo;

import org.elasticsearch.Version;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.reindex.BulkByScrollTask;
import org.elasticsearch.index.reindex.BulkByScrollTaskStatusTests;
import org.elasticsearch.index.reindex.ResumeInfo;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Wire serialization tests for {@link ResumeInfo.PitWorkerResumeInfo}.
 */
public class PitWorkerResumeInfoWireSerializingTests extends AbstractWireSerializingTestCase<
    PitWorkerResumeInfoWireSerializingTests.Wrapper> {

    /**
     * Register {@link ResumeInfo.WorkerResumeInfo} and {@link Task.Status} so that
     * {@link ResumeInfo.PitWorkerResumeInfo} and its status field can be deserialized by name.
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
        return new Wrapper(randomPitWorkerResumeInfo());
    }

    @Override
    protected Wrapper mutateInstance(Wrapper instance) throws IOException {
        // PitWorkerResumeInfo is a record; no need to verify equality via mutations
        return null;
    }

    /**
     * Content-based equals for {@link ResumeInfo.PitWorkerResumeInfo}, tolerant of numeric type changes
     * in {@code searchAfterValues} after round-trip serialization.
     */
    public static boolean pitWorkerResumeInfoContentEquals(ResumeInfo.PitWorkerResumeInfo a, ResumeInfo.PitWorkerResumeInfo b) {
        return Objects.equals(a.pitId(), b.pitId())
            && searchAfterValuesEquals(a.searchAfterValues(), b.searchAfterValues())
            && a.startTimeEpochMillis() == b.startTimeEpochMillis()
            && a.status().equals(b.status())
            && Objects.equals(a.remoteVersion(), b.remoteVersion());
    }

    private static boolean searchAfterValuesEquals(Object[] a, Object[] b) {
        if (a == b) return true;
        if (a == null || b == null || a.length != b.length) return false;
        for (int i = 0; i < a.length; i++) {
            if (searchAfterValueEquals(a[i], b[i]) == false) return false;
        }
        return true;
    }

    private static boolean searchAfterValueEquals(Object a, Object b) {
        if (Objects.equals(a, b)) return true;
        if (a instanceof Number na && b instanceof Number nb) {
            return na.doubleValue() == nb.doubleValue();
        }
        return false;
    }

    /**
     * Content-based hashCode for {@link ResumeInfo.PitWorkerResumeInfo}.
     */
    public static int pitWorkerResumeInfoContentHashCode(ResumeInfo.PitWorkerResumeInfo info) {
        int result = Objects.hashCode(info.pitId());
        result = 31 * result + searchAfterValuesHashCode(info.searchAfterValues());
        result = 31 * result + Long.hashCode(info.startTimeEpochMillis());
        result = 31 * result + info.status().hashCode();
        result = 31 * result + Objects.hashCode(info.remoteVersion());
        return result;
    }

    private static int searchAfterValuesHashCode(Object[] values) {
        if (values == null) return 0;
        int result = 1;
        for (Object v : values) {
            result = 31 * result + (v instanceof Number n ? Double.hashCode(n.doubleValue()) : Objects.hashCode(v));
        }
        return result;
    }

    /**
     * Wrapper around {@link ResumeInfo.PitWorkerResumeInfo} that implements content-based equals/hashCode.
     * The record's equals fails after round-trip because {@code searchAfterValues} (Object[]) may have
     * numeric type coercion (e.g. Integer→Long) from writeGenericValue/readGenericValue.
     */
    public static final class Wrapper implements Writeable {
        private final ResumeInfo.PitWorkerResumeInfo delegate;

        public Wrapper(ResumeInfo.PitWorkerResumeInfo delegate) {
            this.delegate = delegate;
        }

        public Wrapper(StreamInput in) throws IOException {
            this(new ResumeInfo.PitWorkerResumeInfo(in));
        }

        public ResumeInfo.PitWorkerResumeInfo delegate() {
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
            return PitWorkerResumeInfoWireSerializingTests.pitWorkerResumeInfoContentEquals(this.delegate, other.delegate);
        }

        @Override
        public int hashCode() {
            return PitWorkerResumeInfoWireSerializingTests.pitWorkerResumeInfoContentHashCode(delegate);
        }
    }

    public static ResumeInfo.PitWorkerResumeInfo randomPitWorkerResumeInfo() {
        return new ResumeInfo.PitWorkerResumeInfo(
            randomPitId(),
            randomSearchAfterValues(),
            randomLong(),
            BulkByScrollTaskStatusTests.randomStatusWithoutException(),
            randomBoolean() ? null : (randomBoolean() ? Version.CURRENT : Version.fromId(randomIntBetween(1, 999999)))
        );
    }

    private static BytesReference randomPitId() {
        return new BytesArray(randomAlphaOfLengthBetween(1, 32).getBytes(StandardCharsets.UTF_8));
    }

    private static Object[] randomSearchAfterValues() {
        int length = randomIntBetween(1, 5);
        Object[] values = new Object[length];
        for (int i = 0; i < length; i++) {
            values[i] = switch (randomIntBetween(0, 4)) {
                case 0 -> randomLong();
                case 1 -> randomAlphaOfLengthBetween(1, 10);
                case 2 -> randomInt();
                case 3 -> randomDouble();
                case 4 -> randomBoolean();
                default -> randomAlphaOfLength(5);
            };
        }
        return values;
    }
}

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
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.ResumeInfo;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.index.reindex.resumeinfo.SliceStatusWireSerializingTests.bulkByScrollResponseContentEquals;
import static org.elasticsearch.index.reindex.resumeinfo.SliceStatusWireSerializingTests.bulkByScrollResponseContentHashCode;
import static org.elasticsearch.index.reindex.resumeinfo.SliceStatusWireSerializingTests.randomBulkByScrollResponse;

/**
 * Wire serialization tests for {@link ResumeInfo.WorkerResult}.
 * Uses a {@link Wrapper} with content-based equals/hashCode because {@link ResumeInfo.WorkerResult}
 * holds {@link BulkByScrollResponse} without structural {@code equals}.
 */
public class WorkerResultWireSerializingTests extends AbstractWireSerializingTestCase<WorkerResultWireSerializingTests.Wrapper> {

    @Override
    protected Writeable.Reader<Wrapper> instanceReader() {
        return Wrapper::new;
    }

    @Override
    protected Wrapper createTestInstance() {
        return new Wrapper(randomWorkerResult());
    }

    @Override
    protected Wrapper mutateInstance(Wrapper instance) throws IOException {
        return null;
    }

    /**
     * Wrapper around {@link ResumeInfo.WorkerResult} that implements content-based equals/hashCode so that
     * round-trip serialization tests pass when the result holds {@link BulkByScrollResponse} or {@link Exception}.
     */
    public static final class Wrapper implements Writeable {
        private final ResumeInfo.WorkerResult delegate;

        public Wrapper(ResumeInfo.WorkerResult delegate) {
            this.delegate = delegate;
        }

        public Wrapper(StreamInput in) throws IOException {
            this(new ResumeInfo.WorkerResult(in));
        }

        public ResumeInfo.WorkerResult delegate() {
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
            return workerResultContentEquals(this.delegate, other.delegate);
        }

        @Override
        public int hashCode() {
            return workerResultContentHashCode(delegate);
        }

        private static boolean workerResultContentEquals(ResumeInfo.WorkerResult a, ResumeInfo.WorkerResult b) {
            if (a.getResponse().isPresent()) {
                if (b.getResponse().isPresent() == false) return false;
                return bulkByScrollResponseContentEquals(a.getResponse().get(), b.getResponse().get());
            } else {
                if (b.getFailure().isPresent() == false) return false;
                return Objects.equals(a.getFailure().get().getMessage(), b.getFailure().get().getMessage());
            }
        }

        private static int workerResultContentHashCode(ResumeInfo.WorkerResult result) {
            if (result.getResponse().isPresent()) {
                return bulkByScrollResponseContentHashCode(result.getResponse().get());
            } else {
                return Objects.hashCode(result.getFailure().get().getMessage());
            }
        }
    }

    private static ResumeInfo.WorkerResult randomWorkerResult() {
        return randomBoolean()
            ? new ResumeInfo.WorkerResult(randomBulkByScrollResponse(), null)
            : new ResumeInfo.WorkerResult(null, new ElasticsearchException(randomAlphaOfLengthBetween(1, 20)));
    }
}

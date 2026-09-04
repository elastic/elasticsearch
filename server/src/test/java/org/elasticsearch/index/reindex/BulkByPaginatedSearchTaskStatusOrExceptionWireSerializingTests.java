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
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;
import java.util.Objects;

/**
 * Wire serialization tests for {@link BulkByPaginatedSearchTask.StatusOrException}.
 * Uses a wrapper type so that equality after round-trip is semantic (e.g. exception
 * messages instead of instance identity), matching
 * {@link BulkByPaginatedSearchTaskStatusOrExceptionTests#assertEqualStatusOrException}.
 */
public class BulkByPaginatedSearchTaskStatusOrExceptionWireSerializingTests extends AbstractWireSerializingTestCase<
    BulkByPaginatedSearchTaskStatusOrExceptionWireSerializingTests.StatusOrExceptionWrapper> {

    @Override
    protected Writeable.Reader<StatusOrExceptionWrapper> instanceReader() {
        return StatusOrExceptionWrapper::new;
    }

    @Override
    protected StatusOrExceptionWrapper createTestInstance() {
        BulkByPaginatedSearchTask.StatusOrException statusOrException = BulkByPaginatedSearchTaskStatusOrExceptionTests
            .createTestInstanceWithExceptions();
        return new StatusOrExceptionWrapper(statusOrException);
    }

    @Override
    protected StatusOrExceptionWrapper mutateInstance(StatusOrExceptionWrapper instance) throws IOException {
        BulkByPaginatedSearchTask.StatusOrException statusOrException = instance.statusOrException;
        int field = between(0, 1);
        if (field == 0) {
            if (statusOrException.getStatus() != null) {
                return new StatusOrExceptionWrapper(
                    new BulkByPaginatedSearchTask.StatusOrException(
                        BulkByPaginatedSearchTaskStatusWireSerializingTests.mutateStatus(statusOrException.getStatus())
                    )
                );
            } else {
                return new StatusOrExceptionWrapper(
                    new BulkByPaginatedSearchTask.StatusOrException(BulkByPaginatedSearchTaskStatusTests.randomStatus())
                );
            }
        } else {
            if (statusOrException.getException() != null) {
                Exception currentException = statusOrException.getException();
                Exception differentException = randomValueOtherThan(
                    currentException,
                    () -> new ElasticsearchException(randomAlphaOfLengthBetween(5, 15))
                );
                return new StatusOrExceptionWrapper(new BulkByPaginatedSearchTask.StatusOrException(differentException));
            } else {
                return new StatusOrExceptionWrapper(
                    new BulkByPaginatedSearchTask.StatusOrException(new ElasticsearchException(randomAlphaOfLengthBetween(5, 15)))
                );
            }
        }
    }

    @Override
    protected void assertEqualInstances(StatusOrExceptionWrapper expectedInstance, StatusOrExceptionWrapper newInstance) {
        assertNotSame(expectedInstance, newInstance);
        BulkByPaginatedSearchTaskStatusOrExceptionTests.assertEqualStatusOrException(
            expectedInstance.statusOrException,
            newInstance.statusOrException,
            true,
            true
        );
    }

    /**
     * Wrapper around {@link BulkByPaginatedSearchTask.StatusOrException} that implements semantic equality and hashCode
     * so that round-trip serialization passes (e.g. exceptions are compared by message, not reference).
     */
    static class StatusOrExceptionWrapper implements Writeable {
        private final BulkByPaginatedSearchTask.StatusOrException statusOrException;

        StatusOrExceptionWrapper(BulkByPaginatedSearchTask.StatusOrException statusOrException) {
            this.statusOrException = statusOrException;
        }

        StatusOrExceptionWrapper(StreamInput in) throws IOException {
            this.statusOrException = new BulkByPaginatedSearchTask.StatusOrException(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            statusOrException.writeTo(out);
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) return true;
            if (other == null || getClass() != other.getClass()) return false;
            StatusOrExceptionWrapper that = (StatusOrExceptionWrapper) other;
            return statusOrExceptionEquals(statusOrException, that.statusOrException);
        }

        @Override
        public int hashCode() {
            return statusOrExceptionHashCode(statusOrException);
        }

        private static boolean statusOrExceptionEquals(
            BulkByPaginatedSearchTask.StatusOrException first,
            BulkByPaginatedSearchTask.StatusOrException second
        ) {
            if (first == second) return true;
            if (first == null || second == null) return false;
            if (first.getStatus() != null && second.getStatus() != null) {
                return BulkByPaginatedSearchTaskStatusWireSerializingTests.StatusWrapper.statusEquals(
                    first.getStatus(),
                    second.getStatus()
                );
            }
            if (first.getException() != null && second.getException() != null) {
                return Objects.equals(first.getException().getMessage(), second.getException().getMessage());
            }
            return false;
        }

        private static int statusOrExceptionHashCode(BulkByPaginatedSearchTask.StatusOrException statusOrException) {
            if (statusOrException == null) return 0;
            if (statusOrException.getStatus() != null) {
                return BulkByPaginatedSearchTaskStatusWireSerializingTests.StatusWrapper.statusHashCode(statusOrException.getStatus());
            }
            if (statusOrException.getException() != null) {
                return Objects.hashCode(statusOrException.getException().getMessage());
            }
            return 0;
        }
    }
}

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
 * Wire serialization tests for {@link BulkByScrollTask.StatusOrException}.
 * Uses a wrapper type so that equality after round-trip is semantic (e.g. exception
 * messages instead of instance identity), matching
 * {@link BulkByScrollTaskStatusOrExceptionTests#assertEqualStatusOrException}.
 */
public class BulkByScrollTaskStatusOrExceptionWireSerializingTests extends AbstractWireSerializingTestCase<
    BulkByScrollTaskStatusOrExceptionWireSerializingTests.StatusOrExceptionWrapper> {

    @Override
    protected Writeable.Reader<StatusOrExceptionWrapper> instanceReader() {
        return StatusOrExceptionWrapper::new;
    }

    @Override
    protected StatusOrExceptionWrapper createTestInstance() {
        BulkByScrollTask.StatusOrException statusOrException = BulkByScrollTaskStatusOrExceptionTests.createTestInstanceWithExceptions();
        return new StatusOrExceptionWrapper(statusOrException);
    }

    @Override
    protected StatusOrExceptionWrapper mutateInstance(StatusOrExceptionWrapper instance) throws IOException {
        BulkByScrollTask.StatusOrException statusOrException = instance.statusOrException;
        int field = between(0, 1);
        if (field == 0) {
            if (statusOrException.getStatus() != null) {
                return new StatusOrExceptionWrapper(
                    new BulkByScrollTask.StatusOrException(
                        BulkByScrollTaskStatusWireSerializingTests.mutateStatus(statusOrException.getStatus())
                    )
                );
            } else {
                return new StatusOrExceptionWrapper(new BulkByScrollTask.StatusOrException(BulkByScrollTaskStatusTests.randomStatus()));
            }
        } else {
            if (statusOrException.getException() != null) {
                Exception currentException = statusOrException.getException();
                Exception differentException = randomValueOtherThan(
                    currentException,
                    () -> new ElasticsearchException(randomAlphaOfLengthBetween(5, 15))
                );
                return new StatusOrExceptionWrapper(new BulkByScrollTask.StatusOrException(differentException));
            } else {
                return new StatusOrExceptionWrapper(
                    new BulkByScrollTask.StatusOrException(new ElasticsearchException(randomAlphaOfLengthBetween(5, 15)))
                );
            }
        }
    }

    @Override
    protected void assertEqualInstances(StatusOrExceptionWrapper expectedInstance, StatusOrExceptionWrapper newInstance) {
        assertNotSame(expectedInstance, newInstance);
        BulkByScrollTaskStatusOrExceptionTests.assertEqualStatusOrException(
            expectedInstance.statusOrException,
            newInstance.statusOrException,
            true,
            true
        );
    }

    /**
     * Wrapper around {@link BulkByScrollTask.StatusOrException} that implements semantic equality and hashCode
     * so that round-trip serialization passes (e.g. exceptions are compared by message, not reference).
     */
    static class StatusOrExceptionWrapper implements Writeable {
        private final BulkByScrollTask.StatusOrException statusOrException;

        StatusOrExceptionWrapper(BulkByScrollTask.StatusOrException statusOrException) {
            this.statusOrException = statusOrException;
        }

        StatusOrExceptionWrapper(StreamInput in) throws IOException {
            this.statusOrException = new BulkByScrollTask.StatusOrException(in);
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
            BulkByScrollTask.StatusOrException first,
            BulkByScrollTask.StatusOrException second
        ) {
            if (first == second) return true;
            if (first == null || second == null) return false;
            if (first.getStatus() != null && second.getStatus() != null) {
                return BulkByScrollTaskStatusWireSerializingTests.StatusWrapper.statusEquals(first.getStatus(), second.getStatus());
            }
            if (first.getException() != null && second.getException() != null) {
                return Objects.equals(first.getException().getMessage(), second.getException().getMessage());
            }
            return false;
        }

        private static int statusOrExceptionHashCode(BulkByScrollTask.StatusOrException statusOrException) {
            if (statusOrException == null) return 0;
            if (statusOrException.getStatus() != null) {
                return BulkByScrollTaskStatusWireSerializingTests.StatusWrapper.statusHashCode(statusOrException.getStatus());
            }
            if (statusOrException.getException() != null) {
                return Objects.hashCode(statusOrException.getException().getMessage());
            }
            return 0;
        }
    }
}

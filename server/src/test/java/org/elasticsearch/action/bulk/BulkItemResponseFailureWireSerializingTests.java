/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.bulk;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.bulk.BulkItemResponse.Failure;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;
import java.util.Objects;

public class BulkItemResponseFailureWireSerializingTests extends AbstractWireSerializingTestCase<
    BulkItemResponseFailureWireSerializingTests.FailureWrapper> {
    @Override
    protected FailureWrapper createTestInstance() {
        return new FailureWrapper(randomFailure());
    }

    @Override
    protected Writeable.Reader<FailureWrapper> instanceReader() {
        return FailureWrapper::new;
    }

    @Override
    protected FailureWrapper mutateInstance(FailureWrapper instance) {
        Failure failure = instance.failure();
        int fieldToMutate = randomIntBetween(0, 6);
        Failure mutated = switch (fieldToMutate) {
            case 0 -> new Failure(
                randomValueOtherThan(failure.getIndex(), () -> randomAlphaOfLengthBetween(3, 10)),
                failure.getId(),
                failure.getCause(),
                failure.getSeqNo(),
                failure.getTerm(),
                failure.isAborted(),
                failure.getFailureStoreStatus()
            );
            case 1 -> new Failure(
                failure.getIndex(),
                randomValueOtherThan(failure.getId(), () -> randomBoolean() ? randomAlphaOfLengthBetween(3, 10) : null),
                failure.getCause(),
                failure.getSeqNo(),
                failure.getTerm(),
                failure.isAborted(),
                failure.getFailureStoreStatus()
            );
            case 2 -> new Failure(
                failure.getIndex(),
                failure.getId(),
                randomValueOtherThan(failure.getCause(), BulkItemResponseFailureWireSerializingTests::randomException),
                failure.getSeqNo(),
                failure.getTerm(),
                failure.isAborted(),
                failure.getFailureStoreStatus()
            );
            case 3 -> new Failure(
                failure.getIndex(),
                failure.getId(),
                failure.getCause(),
                randomValueOtherThan(
                    failure.getSeqNo(),
                    () -> randomBoolean() ? randomNonNegativeLong() : SequenceNumbers.UNASSIGNED_SEQ_NO
                ),
                failure.getTerm(),
                failure.isAborted(),
                failure.getFailureStoreStatus()
            );
            case 4 -> new Failure(
                failure.getIndex(),
                failure.getId(),
                failure.getCause(),
                failure.getSeqNo(),
                randomValueOtherThan(
                    failure.getTerm(),
                    () -> randomBoolean() ? randomNonNegativeLong() : SequenceNumbers.UNASSIGNED_PRIMARY_TERM
                ),
                failure.isAborted(),
                failure.getFailureStoreStatus()
            );
            case 5 -> new Failure(
                failure.getIndex(),
                failure.getId(),
                failure.getCause(),
                failure.getSeqNo(),
                failure.getTerm(),
                !failure.isAborted(),
                failure.getFailureStoreStatus()
            );
            case 6 -> new Failure(
                failure.getIndex(),
                failure.getId(),
                failure.getCause(),
                failure.getSeqNo(),
                failure.getTerm(),
                failure.isAborted(),
                randomValueOtherThan(failure.getFailureStoreStatus(), () -> randomFrom(IndexDocFailureStoreStatus.values()))
            );
            default -> throw new AssertionError();
        };
        return new FailureWrapper(mutated);
    }

    /**
     * Wrapper around {@link BulkItemResponse.Failure} used solely for wire-serialization testing.
     * <p>
     * {@link AbstractWireSerializingTestCase} relies on {@link Object#equals(Object)} and
     * {@link Object#hashCode()} to verify that instances are preserved across serialization.
     * However, {@link BulkItemResponse.Failure} does not implement semantic equality suitable
     * for this purpose, particularly because it contains an {@link Exception} whose equality
     * is based on object identity rather than type and message.
     * <p>
     * This wrapper provides a stable, semantic {@code equals}/{@code hashCode} implementation
     * that compares the meaningful fields of {@link BulkItemResponse.Failure}, including
     * exception class and message, allowing reliable round-trip wire serialization testing
     * without changing production equality semantics.
     */
    static final class FailureWrapper implements Writeable {
        private final Failure failure;

        FailureWrapper(Failure failure) {
            this.failure = failure;
        }

        FailureWrapper(StreamInput in) throws IOException {
            this.failure = new Failure(in);
        }

        Failure failure() {
            return failure;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            failure.writeTo(out);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            FailureWrapper that = (FailureWrapper) o;
            return failuresEqual(failure, that.failure);
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                failure.getIndex(),
                failure.getId(),
                failure.getStatus(),
                failure.getSeqNo(),
                failure.getTerm(),
                failure.isAborted(),
                failure.getFailureStoreStatus(),
                failure.getCause().getClass(),
                failure.getCause().getMessage()
            );
        }

        public static boolean failuresEqual(Failure a, Failure b) {
            return Objects.equals(a.getIndex(), b.getIndex())
                && Objects.equals(a.getId(), b.getId())
                && a.getSeqNo() == b.getSeqNo()
                && a.getTerm() == b.getTerm()
                && a.isAborted() == b.isAborted()
                && a.getStatus() == b.getStatus()
                && a.getFailureStoreStatus() == b.getFailureStoreStatus()
                && a.getCause().getClass().equals(b.getCause().getClass())
                && Objects.equals(a.getCause().getMessage(), b.getCause().getMessage());
        }
    }

    static Failure randomFailure() {
        String index = randomAlphaOfLengthBetween(3, 10);
        String id = randomBoolean() ? randomAlphaOfLengthBetween(3, 10) : null;
        Exception cause = randomException();
        return new Failure(
            index,
            id,
            cause,
            randomBoolean() ? randomNonNegativeLong() : SequenceNumbers.UNASSIGNED_SEQ_NO,
            randomBoolean() ? randomNonNegativeLong() : SequenceNumbers.UNASSIGNED_PRIMARY_TERM,
            randomBoolean(),
            randomFrom(IndexDocFailureStoreStatus.values())
        );
    }

    static Exception randomException() {
        return randomFrom(
            new IllegalArgumentException(randomAlphaOfLengthBetween(5, 20)),
            new IllegalStateException(randomAlphaOfLengthBetween(5, 20)),
            new ElasticsearchException(randomAlphaOfLengthBetween(5, 20))
        );
    }
}

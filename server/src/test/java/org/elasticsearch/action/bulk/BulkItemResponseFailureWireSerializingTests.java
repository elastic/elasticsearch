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

    /**
     * Mutates a single, representative field of the wrapped {@link BulkItemResponse.Failure}.
     * <p>
     * {@code AbstractWireSerializingTestCase#mutateInstance(Object)} is intended to verify that
     * a serialized instance is not considered equal to a meaningfully different instance. It
     * does <em>not</em> require exhaustive mutation of every field.
     * <p>
     * In this test, only a subset of fields (index, id, cause, and failure store status) are
     * mutated because:
     * <ul>
     *     <li>Other fields (such as {@code seqNo}, {@code term}, and {@code aborted}) are already
     *     extensively exercised through random instance generation and round‑trip
     *     serialization.</li>
     *     <li>Some fields are interdependent or constructor‑driven, and mutating them in
     *     isolation would require reconstructing the {@link Failure} via a different constructor,
     *     adding noise without increasing serialization coverage.</li>
     *     <li>Changing any one of the selected fields is sufficient to ensure that equality
     *     and hash‑based comparisons fail if serialization does not faithfully preserve state.</li>
     * </ul>
     * <p>
     * This approach keeps mutation focused and stable while still providing strong guarantees
     * that wire serialization preserves the observable semantics of {@link Failure}.
     */
    @Override
    protected FailureWrapper mutateInstance(FailureWrapper instance) {
        Failure failure = instance.failure();
        int fieldToMutate = randomIntBetween(0, 3);
        Failure mutated = switch (fieldToMutate) {
            case 0 -> randomFailure(
                randomValueOtherThan(failure.getIndex(), () -> randomAlphaOfLengthBetween(3, 10)),
                failure.getId(),
                failure.getCause(),
                failure.getSeqNo(),
                failure.getTerm(),
                failure.isAborted(),
                failure.getFailureStoreStatus()
            );
            case 1 -> randomFailure(
                failure.getIndex(),
                randomValueOtherThan(failure.getId(), () -> randomBoolean() ? randomAlphaOfLengthBetween(3, 10) : null),
                failure.getCause(),
                failure.getSeqNo(),
                failure.getTerm(),
                failure.isAborted(),
                failure.getFailureStoreStatus()
            );
            case 2 -> randomFailure(
                failure.getIndex(),
                failure.getId(),
                randomValueOtherThan(failure.getCause(), BulkItemResponseFailureWireSerializingTests::randomException),
                failure.getSeqNo(),
                failure.getTerm(),
                failure.isAborted(),
                failure.getFailureStoreStatus()
            );
            case 3 -> randomFailure(
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
     * Custom equality assertion for wire-serialization testing.
     * <p>
     * {@link AbstractWireSerializingTestCase} requires a semantic comparison between
     * the expected and actual instances after a serialization round-trip. For
     * {@link BulkItemResponse.Failure}, default equality semantics are insufficient
     * because it contains an {@link Exception}, whose equality is based on object
     * identity rather than logical content.
     * <p>
     * This method performs a field-by-field comparison of the meaningful state,
     * including exception class and message (but not identity), to ensure that
     * wire serialization faithfully preserves the observable failure information
     * without requiring {@link BulkItemResponse.Failure} to implement or change
     * {@code equals}/{@code hashCode()} semantics in production code.
     */
    @Override
    protected void assertEqualInstances(FailureWrapper expected, FailureWrapper actual) {
        Failure e = expected.failure();
        Failure a = actual.failure();
        assertEquals(e.getIndex(), a.getIndex());
        assertEquals(e.getId(), a.getId());
        assertEquals(e.getSeqNo(), a.getSeqNo());
        assertEquals(e.getTerm(), a.getTerm());
        assertEquals(e.isAborted(), a.isAborted());
        assertEquals(e.getFailureStoreStatus(), a.getFailureStoreStatus());
        assertEquals(e.getStatus(), a.getStatus());
        // Exception comparison: semantic, not identity
        assertEquals(e.getCause().getClass(), a.getCause().getClass());
        assertEquals(e.getCause().getMessage(), a.getCause().getMessage());
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

        private static boolean failuresEqual(Failure a, Failure b) {
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
        return randomFailure(
            index,
            id,
            cause,
            randomBoolean() ? randomNonNegativeLong() : SequenceNumbers.UNASSIGNED_SEQ_NO,
            randomBoolean() ? randomNonNegativeLong() : SequenceNumbers.UNASSIGNED_PRIMARY_TERM,
            randomBoolean(),
            randomFrom(IndexDocFailureStoreStatus.values())
        );
    }

    static Failure randomFailure(
        String index,
        String id,
        Exception cause,
        long seqNo,
        long term,
        boolean aborted,
        IndexDocFailureStoreStatus failureStoreStatus
    ) {
        Failure failure;
        if (seqNo != SequenceNumbers.UNASSIGNED_SEQ_NO || term != SequenceNumbers.UNASSIGNED_PRIMARY_TERM) {
            failure = new Failure(index, id, cause, seqNo, term);
        } else if (aborted) {
            failure = new Failure(index, id, cause, true);
        } else if (failureStoreStatus != IndexDocFailureStoreStatus.NOT_APPLICABLE_OR_UNKNOWN) {
            failure = new Failure(index, id, cause, failureStoreStatus);
        } else {
            failure = new Failure(index, id, cause);
        }
        failure.setFailureStoreStatus(failureStoreStatus);
        return failure;
    }

    static Exception randomException() {
        return randomFrom(
            new IllegalArgumentException(randomAlphaOfLengthBetween(5, 20)),
            new IllegalStateException(randomAlphaOfLengthBetween(5, 20)),
            new ElasticsearchException(randomAlphaOfLengthBetween(5, 20))
        );
    }
}

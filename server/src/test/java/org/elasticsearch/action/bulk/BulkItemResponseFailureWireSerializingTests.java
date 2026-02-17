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
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

public class BulkItemResponseFailureWireSerializingTests extends AbstractWireSerializingTestCase<Failure> {
    @Override
    protected Failure createTestInstance() {
        return randomFailure();
    }

    @Override
    protected Writeable.Reader<Failure> instanceReader() {
        return Failure::new;
    }

    /**
     * Returns a mutated copy of the given {@link BulkItemResponse.Failure}.
     * <p>
     * The mutation modifies exactly one field that participates in semantic
     * equality. Only fields whose values survive public constructor normalization
     * are mutated (such as {@code index}, {@code id}, {@code cause}, and
     * {@code failureStoreStatus}).
     * <p>
     * Other fields (for example {@code seqNo}, {@code term}, and {@code aborted})
     * are intentionally not mutated here because their values may be implicitly
     * reset or derived by the available public constructors, which can result in
     * mutations that are semantically equivalent to the original instance. Such
     * mutations would violate the expectations of {@code testEqualsAndHashcode}.
     * <p>
     * This guarantees that the mutated instance is always unequal to the original
     * according to {@link Object#equals(Object)}, while still modifying exactly one
     * logical field per invocation.
     */
    @Override
    protected Failure mutateInstance(Failure instance) {
        int fieldToMutate = randomIntBetween(0, 3);
        switch (fieldToMutate) {
            case 0 -> {
                String newIndex = randomValueOtherThan(instance.getIndex(), () -> randomAlphaOfLengthBetween(3, 10));
                return randomFailure(
                    newIndex,
                    instance.getId(),
                    instance.getCause(),
                    instance.getSeqNo(),
                    instance.getTerm(),
                    instance.isAborted(),
                    instance.getFailureStoreStatus()
                );
            }
            case 1 -> {
                String newId = randomValueOtherThan(instance.getId(), () -> randomBoolean() ? randomAlphaOfLengthBetween(3, 10) : null);
                return randomFailure(
                    instance.getIndex(),
                    newId,
                    instance.getCause(),
                    instance.getSeqNo(),
                    instance.getTerm(),
                    instance.isAborted(),
                    instance.getFailureStoreStatus()
                );
            }
            case 2 -> {
                Exception newCause = randomValueOtherThan(
                    instance.getCause(),
                    BulkItemResponseFailureWireSerializingTests::randomException
                );
                return randomFailure(
                    instance.getIndex(),
                    instance.getId(),
                    newCause,
                    instance.getSeqNo(),
                    instance.getTerm(),
                    instance.isAborted(),
                    instance.getFailureStoreStatus()
                );
            }
            case 3 -> {
                IndexDocFailureStoreStatus newStatus = randomValueOtherThan(
                    instance.getFailureStoreStatus(),
                    () -> randomFrom(IndexDocFailureStoreStatus.values())
                );
                return randomFailure(
                    instance.getIndex(),
                    instance.getId(),
                    instance.getCause(),
                    instance.getSeqNo(),
                    instance.getTerm(),
                    instance.isAborted(),
                    newStatus
                );
            }
            default -> throw new AssertionError();
        }
    }

    /**
     * Asserts semantic equality between two {@link BulkItemResponse.Failure} instances.
     *
     * <p>
     * {@link BulkItemResponse.Failure} contains an {@link Exception} which is
     * re-created during wire deserialization, so the original and deserialized
     * instances cannot be compared using object identity.
     * </p>
     *
     * <p>
     * This method compares all wire-relevant fields explicitly and compares the
     * failure cause by exception type and message only, which is sufficient to
     * verify correct wire serialization while avoiding brittle identity-based
     * comparisons.
     * </p>
     */
    @Override
    protected void assertEqualInstances(Failure expected, Failure actual) {
        assertEquals(expected.getIndex(), actual.getIndex());
        assertEquals(expected.getId(), actual.getId());
        assertEquals(expected.getSeqNo(), actual.getSeqNo());
        assertEquals(expected.getTerm(), actual.getTerm());
        assertEquals(expected.isAborted(), actual.isAborted());
        assertEquals(expected.getFailureStoreStatus(), actual.getFailureStoreStatus());
        assertEquals(expected.getStatus(), actual.getStatus());
        // Compare exception semantically, not by identity
        assertEquals(expected.getCause().getClass(), actual.getCause().getClass());
        assertEquals(expected.getCause().getMessage(), actual.getCause().getMessage());
    }

    public static Failure randomFailure() {
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

    private static Failure randomFailure(
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

    public static Exception randomException() {
        return randomFrom(
            new IllegalArgumentException(randomAlphaOfLengthBetween(5, 20)),
            new IllegalStateException(randomAlphaOfLengthBetween(5, 20)),
            new ElasticsearchException(randomAlphaOfLengthBetween(5, 20))
        );
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.reindex;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.index.reindex.PaginatedSearchFailureTests.randomException;

public class PaginatedSearchFailureWireSerialisationTests extends AbstractWireSerializingTestCase<
    PaginatedSearchFailureWireSerialisationTests.PaginatedSearchFailureWrapper> {
    @Override
    protected PaginatedSearchFailureWrapper createTestInstance() {
        Throwable reason = randomException();
        String index = randomBoolean() ? randomAlphaOfLengthBetween(1, 10) : null;
        Integer shardId = randomBoolean() ? randomIntBetween(0, 100) : null;
        String nodeId = randomBoolean() ? randomAlphaOfLengthBetween(1, 10) : null;
        return new PaginatedSearchFailureWrapper(new PaginatedSearchFailure(reason, index, shardId, nodeId));
    }

    @Override
    protected Writeable.Reader<PaginatedSearchFailureWrapper> instanceReader() {
        return PaginatedSearchFailureWrapper::new;
    }

    @Override
    protected PaginatedSearchFailureWrapper mutateInstance(PaginatedSearchFailureWrapper instance) {
        return new PaginatedSearchFailureWrapper(mutateSearchFailure(instance.failure()));
    }

    /**
     * Wrapper around {@link PaginatedSearchFailure} used exclusively for wire-serialization tests.
     * <p>
     * {@link AbstractWireSerializingTestCase} requires instances to be comparable via
     * {@code equals}/{@code hashCode()}, but {@link PaginatedSearchFailure} does not define
     * suitable semantic equality due to its embedded {@link Throwable}.
     * <p>
     * This wrapper provides stable, test-only equality semantics without leaking
     * test concerns into production code.
     */
    static final class PaginatedSearchFailureWrapper implements Writeable {
        private final PaginatedSearchFailure failure;

        PaginatedSearchFailureWrapper(PaginatedSearchFailure failure) {
            this.failure = failure;
        }

        PaginatedSearchFailureWrapper(StreamInput in) throws IOException {
            this.failure = new PaginatedSearchFailure(in);
        }

        PaginatedSearchFailure failure() {
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
            PaginatedSearchFailureWrapper that = (PaginatedSearchFailureWrapper) o;
            return failuresEqual(failure, that.failure);
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                failure.getIndex(),
                failure.getShardId(),
                failure.getNodeId(),
                failure.getStatus(),
                failure.getReason().getClass(),
                failure.getReason().getMessage()
            );
        }

        private static boolean failuresEqual(PaginatedSearchFailure a, PaginatedSearchFailure b) {
            return Objects.equals(a.getIndex(), b.getIndex())
                && Objects.equals(a.getShardId(), b.getShardId())
                && Objects.equals(a.getNodeId(), b.getNodeId())
                && a.getStatus() == b.getStatus()
                && a.getReason().getClass().equals(b.getReason().getClass())
                && Objects.equals(a.getReason().getMessage(), b.getReason().getMessage());
        }
    }

    static PaginatedSearchFailure mutateSearchFailure(PaginatedSearchFailure instance) {
        int fieldToMutate = randomIntBetween(0, 3);
        return switch (fieldToMutate) {
            case 0 -> {
                Throwable newReason;
                do {
                    newReason = randomException();
                } while (newReason.getClass().equals(instance.getReason().getClass())
                    && Objects.equals(newReason.getMessage(), instance.getReason().getMessage()));
                yield new PaginatedSearchFailure(
                    newReason,
                    instance.getIndex(),
                    instance.getShardId(),
                    instance.getNodeId(),
                    ExceptionsHelper.status(newReason)
                );
            }
            case 1 -> {
                String newIndex = instance.getIndex() == null
                    ? randomAlphaOfLengthBetween(1, 10)
                    : randomValueOtherThan(instance.getIndex(), () -> randomAlphaOfLengthBetween(1, 10));
                yield new PaginatedSearchFailure(
                    instance.getReason(),
                    newIndex,
                    instance.getShardId(),
                    instance.getNodeId(),
                    instance.getStatus()
                );
            }
            case 2 -> {
                Integer newShardId = instance.getShardId() == null
                    ? randomIntBetween(0, 100)
                    : randomValueOtherThan(instance.getShardId(), () -> randomIntBetween(0, 100));
                yield new PaginatedSearchFailure(
                    instance.getReason(),
                    instance.getIndex(),
                    newShardId,
                    instance.getNodeId(),
                    instance.getStatus()
                );
            }
            case 3 -> {
                String newNodeId = instance.getNodeId() == null
                    ? randomAlphaOfLengthBetween(1, 10)
                    : randomValueOtherThan(instance.getNodeId(), () -> randomAlphaOfLengthBetween(1, 10));
                yield new PaginatedSearchFailure(
                    instance.getReason(),
                    instance.getIndex(),
                    instance.getShardId(),
                    newNodeId,
                    instance.getStatus()
                );
            }
            default -> throw new AssertionError("Unknown field index [" + fieldToMutate + "]");
        };
    }
}

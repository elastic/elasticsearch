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

import static org.elasticsearch.index.reindex.BulkByPaginatedSearchFailureTests.randomException;

public class BulkByPaginatedSearchFailureWireSerialisationTests extends AbstractWireSerializingTestCase<
    BulkByPaginatedSearchFailureWireSerialisationTests.BulkByPaginatedSearchFailureWrapper> {
    @Override
    protected BulkByPaginatedSearchFailureWrapper createTestInstance() {
        Throwable reason = randomException();
        String index = randomBoolean() ? randomAlphaOfLengthBetween(1, 10) : null;
        Integer shardId = randomBoolean() ? randomIntBetween(0, 100) : null;
        String nodeId = randomBoolean() ? randomAlphaOfLengthBetween(1, 10) : null;
        return new BulkByPaginatedSearchFailureWrapper(new BulkByPaginatedSearchFailure(reason, index, shardId, nodeId));
    }

    @Override
    protected Writeable.Reader<BulkByPaginatedSearchFailureWrapper> instanceReader() {
        return BulkByPaginatedSearchFailureWrapper::new;
    }

    @Override
    protected BulkByPaginatedSearchFailureWrapper mutateInstance(BulkByPaginatedSearchFailureWrapper instance) {
        return new BulkByPaginatedSearchFailureWrapper(mutateSearchFailure(instance.failure()));
    }

    /**
     * Wrapper around {@link BulkByPaginatedSearchFailure} used exclusively for wire-serialization tests.
     * <p>
     * {@link AbstractWireSerializingTestCase} requires instances to be comparable via
     * {@code equals}/{@code hashCode()}, but {@link BulkByPaginatedSearchFailure} does not define
     * suitable semantic equality due to its embedded {@link Throwable}.
     * <p>
     * This wrapper provides stable, test-only equality semantics without leaking
     * test concerns into production code.
     */
    static final class BulkByPaginatedSearchFailureWrapper implements Writeable {
        private final BulkByPaginatedSearchFailure failure;

        BulkByPaginatedSearchFailureWrapper(BulkByPaginatedSearchFailure failure) {
            this.failure = failure;
        }

        BulkByPaginatedSearchFailureWrapper(StreamInput in) throws IOException {
            this.failure = new BulkByPaginatedSearchFailure(in);
        }

        BulkByPaginatedSearchFailure failure() {
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
            BulkByPaginatedSearchFailureWrapper that = (BulkByPaginatedSearchFailureWrapper) o;
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

        private static boolean failuresEqual(BulkByPaginatedSearchFailure a, BulkByPaginatedSearchFailure b) {
            return Objects.equals(a.getIndex(), b.getIndex())
                && Objects.equals(a.getShardId(), b.getShardId())
                && Objects.equals(a.getNodeId(), b.getNodeId())
                && a.getStatus() == b.getStatus()
                && a.getReason().getClass().equals(b.getReason().getClass())
                && Objects.equals(a.getReason().getMessage(), b.getReason().getMessage());
        }
    }

    static BulkByPaginatedSearchFailure mutateSearchFailure(BulkByPaginatedSearchFailure instance) {
        int fieldToMutate = randomIntBetween(0, 3);
        return switch (fieldToMutate) {
            case 0 -> {
                Throwable newReason;
                do {
                    newReason = randomException();
                } while (newReason.getClass().equals(instance.getReason().getClass())
                    && Objects.equals(newReason.getMessage(), instance.getReason().getMessage()));
                yield new BulkByPaginatedSearchFailure(
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
                yield new BulkByPaginatedSearchFailure(
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
                yield new BulkByPaginatedSearchFailure(
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
                yield new BulkByPaginatedSearchFailure(
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

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.reindex.paginatedhitsource;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.reindex.PaginatedHitSource.SearchFailure;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import static org.elasticsearch.index.reindex.paginatedhitsource.SearchFailureTests.randomException;

public class SearchFailureWireSerialisationTests extends AbstractWireSerializingTestCase<SearchFailure> {

    @Override
    protected SearchFailure createTestInstance() {
        Throwable reason = randomException();
        String index = randomBoolean() ? randomAlphaOfLengthBetween(1, 10) : null;
        Integer shardId = randomBoolean() ? randomIntBetween(0, 100) : null;
        String nodeId = randomBoolean() ? randomAlphaOfLengthBetween(1, 10) : null;
        return new SearchFailure(reason, index, shardId, nodeId);
    }

    @Override
    protected Writeable.Reader<SearchFailure> instanceReader() {
        return SearchFailure::new;
    }

    @Override
    protected SearchFailure mutateInstance(SearchFailure instance) {
        return mutateSearchFailure(instance);
    }

    /**
     * {@link SearchFailure} contains a {@code Throwable reason}.
     * While the XContent output looks identical, the exception instances are not semantically identical after deserialization.
     * Therefore, we must provide our own equality override to verify that the exceptions are meaningfully identical, even if the instance
     * equality check fails.
     */
    @Override
    protected void assertEqualInstances(SearchFailure expected, SearchFailure actual) {
        assertEquals(expected.getIndex(), actual.getIndex());
        assertEquals(expected.getShardId(), actual.getShardId());
        assertEquals(expected.getNodeId(), actual.getNodeId());
        assertEquals(expected.getStatus(), actual.getStatus());

        // Compare exception meaningfully, not by identity
        assertEquals(expected.getReason().getClass(), actual.getReason().getClass());
        assertEquals(expected.getReason().getMessage(), actual.getReason().getMessage());
    }

    public static SearchFailure mutateSearchFailure(SearchFailure instance) {
        int fieldToMutate = randomIntBetween(0, 3);
        switch (fieldToMutate) {
            case 0 -> {
                Throwable newReason;
                do {
                    newReason = randomException();
                } while (newReason.getClass().equals(instance.getReason().getClass())
                    && String.valueOf(newReason.getMessage()).equals(String.valueOf(instance.getReason().getMessage())));
                return new SearchFailure(
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
                return new SearchFailure(instance.getReason(), newIndex, instance.getShardId(), instance.getNodeId(), instance.getStatus());
            }
            case 2 -> {
                Integer newShardId = instance.getShardId() == null
                    ? randomIntBetween(0, 100)
                    : randomValueOtherThan(instance.getShardId(), () -> randomIntBetween(0, 100));
                return new SearchFailure(instance.getReason(), instance.getIndex(), newShardId, instance.getNodeId(), instance.getStatus());
            }
            case 3 -> {
                String newNodeId = instance.getNodeId() == null
                    ? randomAlphaOfLengthBetween(1, 10)
                    : randomValueOtherThan(instance.getNodeId(), () -> randomAlphaOfLengthBetween(1, 10));
                return new SearchFailure(instance.getReason(), instance.getIndex(), instance.getShardId(), newNodeId, instance.getStatus());
            }
            default -> throw new AssertionError("Unknown field index [" + fieldToMutate + "]");
        }
    }
}

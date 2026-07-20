/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import static org.hamcrest.Matchers.containsString;

public class NodeCacheSizeAndCommitmentsTests extends AbstractWireSerializingTestCase<NodeCacheSizeAndCommitments> {

    @Override
    protected Writeable.Reader<NodeCacheSizeAndCommitments> instanceReader() {
        return NodeCacheSizeAndCommitments::new;
    }

    @Override
    protected NodeCacheSizeAndCommitments createTestInstance() {
        return randomNodeCacheSizeAndCommitments();
    }

    @Override
    protected NodeCacheSizeAndCommitments mutateInstance(NodeCacheSizeAndCommitments instance) {
        return switch (between(0, 2)) {
            case 0 -> new NodeCacheSizeAndCommitments(
                randomValueOtherThan(instance.cacheSizeInBytes(), NodeCacheSizeAndCommitmentsTests::randomNonNegativeLong),
                instance.boostedCacheCommitmentInBytes(),
                instance.unboostedCacheCommitmentInBytes()
            );
            case 1 -> new NodeCacheSizeAndCommitments(
                instance.cacheSizeInBytes(),
                randomValueOtherThan(instance.boostedCacheCommitmentInBytes(), NodeCacheSizeAndCommitmentsTests::randomNonNegativeLong),
                instance.unboostedCacheCommitmentInBytes()
            );
            case 2 -> new NodeCacheSizeAndCommitments(
                instance.cacheSizeInBytes(),
                instance.boostedCacheCommitmentInBytes(),
                randomValueOtherThan(instance.unboostedCacheCommitmentInBytes(), NodeCacheSizeAndCommitmentsTests::randomNonNegativeLong)
            );
            default -> throw new AssertionError("unexpected branch");
        };
    }

    public void testRejectsNegativeValues() {
        AssertionError cacheSizeError = expectThrows(AssertionError.class, () -> new NodeCacheSizeAndCommitments(-1L, 0L, 0L));
        assertThat(cacheSizeError.getMessage(), containsString("cacheSizeInBytes must be non-negative"));

        AssertionError boostedCommitmentError = expectThrows(AssertionError.class, () -> new NodeCacheSizeAndCommitments(0L, -1L, 0L));
        assertThat(boostedCommitmentError.getMessage(), containsString("boostedCacheCommitmentInBytes must be non-negative"));

        AssertionError unboostedCommitmentError = expectThrows(AssertionError.class, () -> new NodeCacheSizeAndCommitments(0L, 0L, -1L));
        assertThat(unboostedCommitmentError.getMessage(), containsString("unboostedCacheCommitmentInBytes must be non-negative"));
    }

    static NodeCacheSizeAndCommitments randomNodeCacheSizeAndCommitments() {
        return new NodeCacheSizeAndCommitments(randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong());
    }
}

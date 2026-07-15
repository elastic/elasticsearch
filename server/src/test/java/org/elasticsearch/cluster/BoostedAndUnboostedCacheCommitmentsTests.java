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

public class BoostedAndUnboostedCacheCommitmentsTests extends AbstractWireSerializingTestCase<BoostedAndUnboostedCacheCommitments> {

    @Override
    protected Writeable.Reader<BoostedAndUnboostedCacheCommitments> instanceReader() {
        return BoostedAndUnboostedCacheCommitments::new;
    }

    @Override
    protected BoostedAndUnboostedCacheCommitments createTestInstance() {
        return new BoostedAndUnboostedCacheCommitments(randomCacheCommitment(), randomCacheCommitment());
    }

    @Override
    protected BoostedAndUnboostedCacheCommitments mutateInstance(BoostedAndUnboostedCacheCommitments instance) {
        if (randomBoolean()) {
            return new BoostedAndUnboostedCacheCommitments(
                randomValueOtherThan(
                    instance.boostedCacheCommitmentInBytes(),
                    BoostedAndUnboostedCacheCommitmentsTests::randomCacheCommitment
                ),
                instance.unboostedCacheCommitmentInBytes()
            );
        } else {
            return new BoostedAndUnboostedCacheCommitments(
                instance.boostedCacheCommitmentInBytes(),
                randomValueOtherThan(
                    instance.unboostedCacheCommitmentInBytes(),
                    BoostedAndUnboostedCacheCommitmentsTests::randomCacheCommitment
                )
            );
        }
    }

    private static long randomCacheCommitment() {
        return randomFrom(BoostedAndUnboostedCacheCommitments.NO_BOOSTED_OR_UNBOOSTED_CACHE_COMMITMENT, randomNonNegativeLong());
    }
}

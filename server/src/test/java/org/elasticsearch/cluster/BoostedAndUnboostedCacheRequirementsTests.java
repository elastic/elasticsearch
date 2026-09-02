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

public class BoostedAndUnboostedCacheRequirementsTests extends AbstractWireSerializingTestCase<BoostedAndUnboostedCacheRequirements> {

    @Override
    protected Writeable.Reader<BoostedAndUnboostedCacheRequirements> instanceReader() {
        return BoostedAndUnboostedCacheRequirements::new;
    }

    @Override
    protected BoostedAndUnboostedCacheRequirements createTestInstance() {
        return new BoostedAndUnboostedCacheRequirements(randomCacheRequirement(), randomCacheRequirement());
    }

    @Override
    protected BoostedAndUnboostedCacheRequirements mutateInstance(BoostedAndUnboostedCacheRequirements instance) {
        if (randomBoolean()) {
            return new BoostedAndUnboostedCacheRequirements(
                randomValueOtherThan(
                    instance.boostedCacheRequirementInBytes(),
                    BoostedAndUnboostedCacheRequirementsTests::randomCacheRequirement
                ),
                instance.unboostedCacheRequirementInBytes()
            );
        } else {
            return new BoostedAndUnboostedCacheRequirements(
                instance.boostedCacheRequirementInBytes(),
                randomValueOtherThan(
                    instance.unboostedCacheRequirementInBytes(),
                    BoostedAndUnboostedCacheRequirementsTests::randomCacheRequirement
                )
            );
        }
    }

    private static long randomCacheRequirement() {
        return randomFrom(BoostedAndUnboostedCacheRequirements.NO_BOOSTED_OR_UNBOOSTED_CACHE_REQUIREMENT, randomNonNegativeLong());
    }
}

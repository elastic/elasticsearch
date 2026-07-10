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

public class BoostedAndUnboostedCacheSizesTests extends AbstractWireSerializingTestCase<BoostedAndUnboostedCacheSizes> {

    @Override
    protected Writeable.Reader<BoostedAndUnboostedCacheSizes> instanceReader() {
        return BoostedAndUnboostedCacheSizes::new;
    }

    @Override
    protected BoostedAndUnboostedCacheSizes createTestInstance() {
        return new BoostedAndUnboostedCacheSizes(randomCacheSize(), randomCacheSize());
    }

    @Override
    protected BoostedAndUnboostedCacheSizes mutateInstance(BoostedAndUnboostedCacheSizes instance) {
        if (randomBoolean()) {
            return new BoostedAndUnboostedCacheSizes(
                randomValueOtherThan(instance.boostedCacheSizeInBytes(), BoostedAndUnboostedCacheSizesTests::randomCacheSize),
                instance.unboostedCacheSizeInBytes()
            );
        } else {
            return new BoostedAndUnboostedCacheSizes(
                instance.boostedCacheSizeInBytes(),
                randomValueOtherThan(instance.unboostedCacheSizeInBytes(), BoostedAndUnboostedCacheSizesTests::randomCacheSize)
            );
        }
    }

    private static long randomCacheSize() {
        return randomFrom(BoostedAndUnboostedCacheSizes.NO_BOOSTED_OR_UNBOOSTED_CACHE_SIZE, randomNonNegativeLong());
    }
}

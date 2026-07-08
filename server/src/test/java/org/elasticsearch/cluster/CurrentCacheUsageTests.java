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

public class CurrentCacheUsageTests extends AbstractWireSerializingTestCase<CurrentCacheUsage> {

    @Override
    protected Writeable.Reader<CurrentCacheUsage> instanceReader() {
        return CurrentCacheUsage::new;
    }

    @Override
    protected CurrentCacheUsage createTestInstance() {
        return new CurrentCacheUsage(randomNonNegativeLong(), randomNonNegativeLong());
    }

    @Override
    protected CurrentCacheUsage mutateInstance(CurrentCacheUsage instance) {
        return switch (between(0, 2)) {
            case 0 -> new CurrentCacheUsage(
                randomValueOtherThan(instance.cacheSizeInBytes(), CurrentCacheUsageTests::randomNonNegativeLong),
                instance.currentCacheCommitmentInBytes()
            );
            case 1 -> new CurrentCacheUsage(
                instance.cacheSizeInBytes(),
                randomValueOtherThan(instance.currentCacheCommitmentInBytes(), CurrentCacheUsageTests::randomNonNegativeLong)
            );
            case 2 -> new CurrentCacheUsage(
                randomValueOtherThan(instance.cacheSizeInBytes(), CurrentCacheUsageTests::randomNonNegativeLong),
                randomValueOtherThan(instance.currentCacheCommitmentInBytes(), CurrentCacheUsageTests::randomNonNegativeLong)
            );
            default -> throw new AssertionError("unexpected mutation branch");
        };
    }
}

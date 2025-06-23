/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.frozen;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

public class FrozenIndicesFeatureSetUsageTests extends AbstractWireSerializingTestCase<FrozenIndicesFeatureSetUsage> {

    @Override
    protected FrozenIndicesFeatureSetUsage createTestInstance() {
        boolean available = randomBoolean();
        boolean enabled = randomBoolean();
        return new FrozenIndicesFeatureSetUsage(available, enabled, randomIntBetween(0, 100000));
    }

    @Override
    protected FrozenIndicesFeatureSetUsage mutateInstance(FrozenIndicesFeatureSetUsage instance) {
        boolean available = instance.available();
        boolean enabled = instance.enabled();
        int numFrozenIndices = instance.getNumberOfFrozenIndices();
        switch (between(0, 2)) {
            case 0 -> available = available == false;
            case 1 -> enabled = enabled == false;
            case 2 -> numFrozenIndices = randomValueOtherThan(numFrozenIndices, () -> randomIntBetween(0, 100000));
            default -> throw new AssertionError("Illegal randomisation branch");
        }
        return new FrozenIndicesFeatureSetUsage(available, enabled, numFrozenIndices);
    }

    @Override
    protected Writeable.Reader<FrozenIndicesFeatureSetUsage> instanceReader() {
        return FrozenIndicesFeatureSetUsage::new;
    }

}

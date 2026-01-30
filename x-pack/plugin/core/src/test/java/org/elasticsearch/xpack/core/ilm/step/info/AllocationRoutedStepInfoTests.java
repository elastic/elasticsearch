/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ilm.step.info;

import org.elasticsearch.test.AbstractXContentTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

public class AllocationRoutedStepInfoTests extends AbstractXContentTestCase<AllocationInfo> {

    @Override
    protected AllocationInfo createTestInstance() {
        return new AllocationInfo(randomNonNegativeLong(), randomNonNegativeLong(), randomBoolean(), randomAlphaOfLengthBetween(5, 10));
    }

    @Override
    protected AllocationInfo doParseInstance(XContentParser parser) throws IOException {
        return AllocationInfo.PARSER.apply(parser, null);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }

    public final void testEqualsAndHashcode() {
        for (int runs = 0; runs < NUMBER_OF_TEST_RUNS; runs++) {
            EqualsHashCodeTestUtils.checkEqualsAndHashCode(createTestInstance(), this::copyInstance, this::mutateInstance);
        }
    }

    protected final AllocationInfo copyInstance(AllocationInfo instance) {
        return new AllocationInfo(
            instance.numberOfReplicas(),
            instance.numberShardsLeftToAllocate(),
            instance.allShardsActive(),
            instance.message()
        );
    }

    protected AllocationInfo mutateInstance(AllocationInfo instance) throws IOException {
        long actualReplicas = instance.numberOfReplicas();
        long shardsToAllocate = instance.numberShardsLeftToAllocate();
        boolean allShardsActive = instance.allShardsActive();
        var message = instance.message();
        switch (between(0, 2)) {
            case 0 -> shardsToAllocate += between(1, 20);
            case 1 -> allShardsActive = allShardsActive == false;
            case 2 -> actualReplicas += between(1, 20);
            default -> throw new AssertionError("Illegal randomisation branch");
        }
        return new AllocationInfo(actualReplicas, shardsToAllocate, allShardsActive, message);
    }

}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.assignment;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;

public class AdaptiveAllocationSettingsTests extends AbstractWireSerializingTestCase<AdaptiveAllocationsSettings> {

    public static AdaptiveAllocationsSettings testInstance() {
        return new AdaptiveAllocationsSettings(
            randomBoolean() ? null : randomBoolean(),
            randomBoolean() ? null : randomIntBetween(1, 2),
            randomBoolean() ? null : randomIntBetween(2, 4)
        );
    }

    public static AdaptiveAllocationsSettings mutate(AdaptiveAllocationsSettings instance) {
        boolean mutatedEnabled = Boolean.FALSE.equals(instance.getEnabled());
        return new AdaptiveAllocationsSettings(mutatedEnabled, instance.getMinNumberOfAllocations(), instance.getMaxNumberOfAllocations());
    }

    @Override
    protected Writeable.Reader<AdaptiveAllocationsSettings> instanceReader() {
        return AdaptiveAllocationsSettings::new;
    }

    @Override
    protected AdaptiveAllocationsSettings createTestInstance() {
        return testInstance();
    }

    @Override
    protected AdaptiveAllocationsSettings mutateInstance(AdaptiveAllocationsSettings instance) throws IOException {
        return mutate(instance);
    }
}

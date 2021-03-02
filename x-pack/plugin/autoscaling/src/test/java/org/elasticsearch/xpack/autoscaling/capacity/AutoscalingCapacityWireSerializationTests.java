/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.autoscaling.capacity;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.autoscaling.AutoscalingTestCase;

import java.io.IOException;

public class AutoscalingCapacityWireSerializationTests extends AbstractWireSerializingTestCase<AutoscalingCapacity> {
    @Override
    protected Writeable.Reader<AutoscalingCapacity> instanceReader() {
        return AutoscalingCapacity::new;
    }

    @Override
    protected AutoscalingCapacity createTestInstance() {
        return AutoscalingTestCase.randomAutoscalingCapacity();
    }

    @Override
    protected AutoscalingCapacity mutateInstance(AutoscalingCapacity instance) throws IOException {
        AutoscalingCapacity.Builder builder = AutoscalingCapacity.builder().capacity(instance);

        if (randomBoolean()) {
            // mutate total
            boolean hasBothStorageAndMemory = instance.total().memory() != null && instance.total().storage() != null;
            if (randomBoolean()) {
                builder.total(
                    randomByteSize(
                        hasBothStorageAndMemory && (instance.node() == null || instance.node().storage() == null),
                        instance.total().storage()
                    ),
                    instance.total().memory()
                );
            } else {
                builder.total(
                    instance.total().storage(),
                    randomByteSize(
                        hasBothStorageAndMemory && (instance.node() == null || instance.node().memory() == null),
                        instance.total().memory()
                    )
                );
            }
        } else {
            // mutate node
            if (instance.node() == null) {
                builder.node(
                    AutoscalingTestCase.randomNullValueAutoscalingResources(
                        instance.total().storage() != null,
                        instance.total().memory() != null
                    )
                );
            } else if (randomBoolean() && instance.total().storage() != null || instance.total().memory() == null) {
                builder.node(randomByteSize(instance.node().memory() != null, instance.node().storage()), instance.node().memory());
            } else {
                builder.node(instance.node().storage(), randomByteSize(instance.node().storage() != null, instance.node().memory()));
            }
        }
        return builder.build();
    }

    private static ByteSizeValue randomByteSize(boolean allowNull, ByteSizeValue original) {
        return randomValueOtherThan(
            original,
            allowNull ? AutoscalingTestCase::randomNullableByteSizeValue : AutoscalingTestCase::randomByteSizeValue
        );
    }
}

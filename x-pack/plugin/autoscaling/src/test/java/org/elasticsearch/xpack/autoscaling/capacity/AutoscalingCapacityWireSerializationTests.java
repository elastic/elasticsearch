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
    protected AutoscalingCapacity mutateInstance(AutoscalingCapacity instance) {
        AutoscalingCapacity.Builder builder = AutoscalingCapacity.builder().capacity(instance);
        if (randomBoolean()) {
            // mutate total
            boolean hasAllMetrics = instance.total().memory() != null
                && instance.total().storage() != null
                && instance.total().processors().isPresent();
            if (randomBoolean()) {
                builder.total(
                    randomByteSize(
                        hasAllMetrics && (instance.node() == null || instance.node().storage() == null),
                        instance.total().storage()
                    ),
                    instance.total().memory(),
                    instance.total().getProcessors()
                );
            } else if (randomBoolean()) {
                builder.total(
                    instance.total().storage(),
                    randomByteSize(
                        hasAllMetrics && (instance.node() == null || instance.node().memory() == null),
                        instance.total().memory()
                    ),
                    instance.total().getProcessors()
                );
            } else {
                builder.total(
                    instance.total().storage(),
                    instance.total().memory(),
                    hasAllMetrics && (instance.node() == null || instance.node().processors().isEmpty()) && randomBoolean()
                        ? null
                        : randomIntBetween(1, 64) + instance.total().processors().orElse(0)
                );
            }
        } else {
            // mutate node
            if (instance.node() == null) {
                builder.node(
                    AutoscalingTestCase.randomNullValueAutoscalingResources(
                        instance.total().storage() != null,
                        instance.total().memory() != null,
                        instance.total().processors().isPresent()
                    )
                );
            } else if (randomBoolean() && instance.total().storage() != null) {
                builder.node(
                    randomByteSize(instance.node().memory() != null || instance.node().processors().isPresent(), instance.node().storage()),
                    instance.node().memory(),
                    instance.node().getProcessors()
                );
            } else if (randomBoolean() && instance.total().memory() != null) {
                builder.node(
                    instance.node().storage(),
                    randomByteSize(instance.node().storage() != null || instance.node().processors().isPresent(), instance.node().memory()),
                    instance.node().getProcessors()
                );
            } else if (instance.total().processors().isPresent()) {
                builder.node(
                    instance.node().storage(),
                    instance.node().memory(),
                    randomBoolean()
                        && (instance.node().storage() != null || instance.node().memory() != null)
                        && instance.node().processors().isPresent()
                            ? null
                            : randomIntBetween(1, 64) + instance.node().processors().orElse(0)
                );
            } else {
                ByteSizeValue newStorage = instance.total().storage() != null
                    ? randomByteSize(
                        instance.node().memory() != null || instance.node().processors().isPresent(),
                        instance.node().storage()
                    )
                    : null;
                ByteSizeValue newMem = instance.total().memory() != null
                    ? randomByteSize(newStorage != null || instance.node().processors().isPresent(), instance.node().memory())
                    : null;
                builder.node(
                    newStorage,
                    newMem,
                    randomBoolean() && (newMem != null || newStorage != null) && instance.node().processors().isPresent() ? null
                        : instance.total().processors().isPresent() && randomBoolean()
                            ? randomIntBetween(1, 64) + instance.node().processors().orElse(0)
                        : null
                );
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

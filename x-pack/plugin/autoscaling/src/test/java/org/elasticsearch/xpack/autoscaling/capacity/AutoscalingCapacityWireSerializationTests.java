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

import java.util.Optional;

import static org.elasticsearch.xpack.autoscaling.AutoscalingTestCase.randomProcessors;

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
                && instance.total().processors() != null;
            if (randomBoolean()) {
                builder.total(
                    randomByteSize(
                        hasAllMetrics && (instance.node() == null || instance.node().storage() == null),
                        instance.total().storage()
                    ),
                    instance.total().memory(),
                    instance.total().processors()
                );
            } else if (randomBoolean()) {
                builder.total(
                    instance.total().storage(),
                    randomByteSize(
                        hasAllMetrics && (instance.node() == null || instance.node().memory() == null),
                        instance.total().memory()
                    ),
                    instance.total().processors()
                );
            } else {
                builder.total(
                    instance.total().storage(),
                    instance.total().memory(),
                    hasAllMetrics && (instance.node() == null || instance.node().processors() == null) && randomBoolean()
                        ? null
                        : Optional.ofNullable(instance.total().processors()).map(p -> p.plus(randomProcessors())).orElse(randomProcessors())
                );
            }
        } else {
            // mutate node
            if (instance.node() == null) {
                builder.node(
                    AutoscalingTestCase.randomNullValueAutoscalingResources(
                        instance.total().storage() != null,
                        instance.total().memory() != null,
                        instance.total().processors() != null
                    )
                );
            } else if (randomBoolean() && instance.total().storage() != null) {
                builder.node(
                    randomByteSize(instance.node().memory() != null || instance.node().processors() != null, instance.node().storage()),
                    instance.node().memory(),
                    instance.node().processors()
                );
            } else if (randomBoolean() && instance.total().memory() != null) {
                builder.node(
                    instance.node().storage(),
                    randomByteSize(instance.node().storage() != null || instance.node().processors() != null, instance.node().memory()),
                    instance.node().processors()
                );
            } else if (instance.total().processors() != null) {
                builder.node(
                    instance.node().storage(),
                    instance.node().memory(),
                    randomBoolean()
                        && (instance.node().storage() != null || instance.node().memory() != null)
                        && instance.node().processors() != null
                            ? null
                            : Optional.ofNullable(instance.total().processors())
                                .map(p -> p.plus(randomProcessors()))
                                .orElse(randomProcessors())
                );
            } else {
                ByteSizeValue newStorage = instance.total().storage() != null
                    ? randomByteSize(instance.node().memory() != null || instance.node().processors() != null, instance.node().storage())
                    : null;
                ByteSizeValue newMem = instance.total().memory() != null
                    ? randomByteSize(newStorage != null || instance.node().processors() != null, instance.node().memory())
                    : null;
                builder.node(
                    newStorage,
                    newMem,
                    randomBoolean() && (newMem != null || newStorage != null) && instance.node().processors() != null ? null
                        : instance.total().processors() != null && randomBoolean()
                            ? Optional.ofNullable(instance.total().processors())
                                .map(p -> p.plus(randomProcessors()))
                                .orElse(randomProcessors())
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

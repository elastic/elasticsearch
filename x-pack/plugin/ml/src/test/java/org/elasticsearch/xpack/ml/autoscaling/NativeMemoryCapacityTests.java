/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.autoscaling;

import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.autoscaling.capacity.AutoscalingCapacity;
import org.elasticsearch.xpack.ml.utils.NativeMemoryCalculator;

import java.util.function.BiConsumer;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class NativeMemoryCapacityTests extends ESTestCase {

    private static final int NUM_TEST_RUNS = 10;

    public void testMerge() {
        NativeMemoryCapacity capacity = new NativeMemoryCapacity(
            ByteSizeValue.ofGb(1).getBytes(),
            ByteSizeValue.ofMb(200).getBytes(),
            ByteSizeValue.ofMb(50).getBytes()
        );
        capacity = capacity.merge(new NativeMemoryCapacity(ByteSizeValue.ofGb(1).getBytes(), ByteSizeValue.ofMb(100).getBytes()));
        assertThat(capacity.getTierMlNativeMemoryRequirement(), equalTo(ByteSizeValue.ofGb(1).getBytes() * 2L));
        assertThat(capacity.getNodeMlNativeMemoryRequirement(), equalTo(ByteSizeValue.ofMb(200).getBytes()));
        assertThat(capacity.getJvmSize(), equalTo(ByteSizeValue.ofMb(50).getBytes()));

        capacity = capacity.merge(new NativeMemoryCapacity(ByteSizeValue.ofGb(1).getBytes(), ByteSizeValue.ofMb(300).getBytes()));

        assertThat(capacity.getTierMlNativeMemoryRequirement(), equalTo(ByteSizeValue.ofGb(1).getBytes() * 3L));
        assertThat(capacity.getNodeMlNativeMemoryRequirement(), equalTo(ByteSizeValue.ofMb(300).getBytes()));
        assertThat(capacity.getJvmSize(), is(nullValue()));
    }

    public void testAutoscalingCapacity() {
        // TODO adjust once future JVM capacity is known
        NativeMemoryCapacity capacity = new NativeMemoryCapacity(
            ByteSizeValue.ofGb(4).getBytes(),
            ByteSizeValue.ofGb(1).getBytes(),
            ByteSizeValue.ofMb(50).getBytes()
        );

        { // auto is false
            AutoscalingCapacity autoscalingCapacity = capacity.autoscalingCapacity(25, false);
            assertThat(autoscalingCapacity.node().memory().getBytes(), equalTo(ByteSizeValue.ofGb(1).getBytes() * 4L));
            assertThat(autoscalingCapacity.total().memory().getBytes(), equalTo(ByteSizeValue.ofGb(4).getBytes() * 4L));
        }
        { // auto is true
            AutoscalingCapacity autoscalingCapacity = capacity.autoscalingCapacity(25, true);
            assertThat(autoscalingCapacity.node().memory().getBytes(), equalTo(1335885824L));
            assertThat(autoscalingCapacity.total().memory().getBytes(), equalTo(5343543296L));
        }
        { // auto is true with unknown jvm size
            capacity = new NativeMemoryCapacity(ByteSizeValue.ofGb(4).getBytes(), ByteSizeValue.ofGb(1).getBytes());
            AutoscalingCapacity autoscalingCapacity = capacity.autoscalingCapacity(25, true);
            assertThat(autoscalingCapacity.node().memory().getBytes(), equalTo(2139095040L));
            assertThat(autoscalingCapacity.total().memory().getBytes(), equalTo(8556380160L));
        }
    }

    public void testAutoscalingCapacityConsistency() {
        final BiConsumer<NativeMemoryCapacity, Integer> consistentAutoAssertions = (nativeMemory, memoryPercentage) -> {
            AutoscalingCapacity autoscalingCapacity = nativeMemory.autoscalingCapacity(25, true);
            assertThat(autoscalingCapacity.total().memory().getBytes(), greaterThan(nativeMemory.getTierMlNativeMemoryRequirement()));
            assertThat(autoscalingCapacity.node().memory().getBytes(), greaterThan(nativeMemory.getNodeMlNativeMemoryRequirement()));
            assertThat(
                autoscalingCapacity.total().memory().getBytes(),
                greaterThanOrEqualTo(autoscalingCapacity.node().memory().getBytes())
            );
        };

        { // 0 memory
            assertThat(
                NativeMemoryCalculator.calculateApproxNecessaryNodeSize(
                    0L,
                    randomLongBetween(0L, ByteSizeValue.ofGb(100).getBytes()),
                    randomIntBetween(0, 100),
                    randomBoolean()
                ),
                equalTo(0L)
            );
            assertThat(
                NativeMemoryCalculator.calculateApproxNecessaryNodeSize(0L, null, randomIntBetween(0, 100), randomBoolean()),
                equalTo(0L)
            );
        }
        for (int i = 0; i < NUM_TEST_RUNS; i++) {
            int memoryPercentage = randomIntBetween(5, 200);
            { // tiny memory
                long nodeMemory = randomLongBetween(ByteSizeValue.ofKb(100).getBytes(), ByteSizeValue.ofMb(500).getBytes());
                consistentAutoAssertions.accept(
                    new NativeMemoryCapacity(randomLongBetween(nodeMemory, nodeMemory * 4), nodeMemory),
                    memoryPercentage
                );
            }
            { // normal-ish memory
                long nodeMemory = randomLongBetween(ByteSizeValue.ofMb(500).getBytes(), ByteSizeValue.ofGb(4).getBytes());
                consistentAutoAssertions.accept(
                    new NativeMemoryCapacity(randomLongBetween(nodeMemory, nodeMemory * 4), nodeMemory),
                    memoryPercentage
                );
            }
            { // huge memory
                long nodeMemory = randomLongBetween(ByteSizeValue.ofGb(30).getBytes(), ByteSizeValue.ofGb(60).getBytes());
                consistentAutoAssertions.accept(
                    new NativeMemoryCapacity(randomLongBetween(nodeMemory, nodeMemory * 4), nodeMemory),
                    memoryPercentage
                );
            }
        }
    }

}

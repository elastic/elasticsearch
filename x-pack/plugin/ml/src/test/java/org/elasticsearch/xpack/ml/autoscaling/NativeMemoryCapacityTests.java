/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.autoscaling;

import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ml.utils.NativeMemoryCalculator;

import java.util.function.BiConsumer;

import static org.elasticsearch.xpack.ml.MachineLearning.NATIVE_EXECUTABLE_CODE_OVERHEAD;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
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
        assertThat(capacity.getTierMlNativeMemoryRequirementExcludingOverhead(), equalTo(ByteSizeValue.ofGb(1).getBytes() * 2L));
        assertThat(capacity.getNodeMlNativeMemoryRequirementExcludingOverhead(), equalTo(ByteSizeValue.ofMb(200).getBytes()));
        // We cannot know the JVM size will stay the same as the bigger tier may lead to bigger nodes
        assertThat(capacity.getJvmSize(), nullValue());

        capacity = capacity.merge(new NativeMemoryCapacity(ByteSizeValue.ofGb(1).getBytes(), ByteSizeValue.ofMb(300).getBytes()));

        assertThat(capacity.getTierMlNativeMemoryRequirementExcludingOverhead(), equalTo(ByteSizeValue.ofGb(1).getBytes() * 3L));
        assertThat(capacity.getNodeMlNativeMemoryRequirementExcludingOverhead(), equalTo(ByteSizeValue.ofMb(300).getBytes()));
        assertThat(capacity.getJvmSize(), nullValue());
    }

    /**
     * This situation arises while finding current capacity when scaling up from zero.
     */
    public void testAutoscalingCapacityFromZero() {

        MlMemoryAutoscalingCapacity autoscalingCapacity = NativeMemoryCapacity.ZERO.autoscalingCapacity(
            randomIntBetween(5, 90),
            randomBoolean(),
            randomLongBetween(100000000L, 10000000000L),
            randomIntBetween(0, 3)
        ).build();
        assertThat(autoscalingCapacity.nodeSize().getBytes(), equalTo(0L));
        assertThat(autoscalingCapacity.tierSize().getBytes(), equalTo(0L));
    }

    public void testAutoscalingCapacity() {

        final long BYTES_IN_64GB = ByteSizeValue.ofGb(64).getBytes();
        final long AUTO_ML_MEMORY_FOR_64GB_NODE = NativeMemoryCalculator.allowedBytesForMl(BYTES_IN_64GB, randomIntBetween(5, 90), true);

        NativeMemoryCapacity capacity = new NativeMemoryCapacity(
            ByteSizeValue.ofGb(4).getBytes() - NATIVE_EXECUTABLE_CODE_OVERHEAD.getBytes(),
            ByteSizeValue.ofGb(1).getBytes() - NATIVE_EXECUTABLE_CODE_OVERHEAD.getBytes(),
            ByteSizeValue.ofMb(50).getBytes()
        );

        // auto is false (which should not be when autoscaling is used as intended)
        {
            MlMemoryAutoscalingCapacity autoscalingCapacity = capacity.autoscalingCapacity(
                25,
                false,
                NativeMemoryCalculator.allowedBytesForMl(BYTES_IN_64GB, 25, false),
                1
            ).build();
            assertThat(autoscalingCapacity.nodeSize().getBytes(), equalTo(ByteSizeValue.ofGb(1).getBytes() * 4L));
            assertThat(autoscalingCapacity.tierSize().getBytes(), equalTo(ByteSizeValue.ofGb(4).getBytes() * 4L));
        }
        // auto is true (so configured max memory percent should be ignored)
        {
            MlMemoryAutoscalingCapacity autoscalingCapacity = capacity.autoscalingCapacity(
                randomIntBetween(5, 90),
                true,
                AUTO_ML_MEMORY_FOR_64GB_NODE,
                1
            ).build();
            assertThat(autoscalingCapacity.nodeSize().getBytes(), equalTo(1335885824L));
            assertThat(autoscalingCapacity.tierSize().getBytes(), equalTo(4557111296L));
        }
        // auto is true with unknown jvm size, memory requirement below JVM size knot point, 1 AZ (this is a realistic case for Cloud)
        {
            capacity = new NativeMemoryCapacity(
                ByteSizeValue.ofGb(4).getBytes() - NATIVE_EXECUTABLE_CODE_OVERHEAD.getBytes(),
                ByteSizeValue.ofGb(1).getBytes() - NATIVE_EXECUTABLE_CODE_OVERHEAD.getBytes()
            );
            MlMemoryAutoscalingCapacity autoscalingCapacity = capacity.autoscalingCapacity(
                randomIntBetween(5, 90),
                true,
                AUTO_ML_MEMORY_FOR_64GB_NODE,
                1
            ).build();
            // 2134900736 bytes = 2036MB
            // 2036MB node => 812MB JVM heap (40% of 2036MB rounded down to a multiple of 4MB)
            // 2036MB - 812MB - 200MB = 1024MB which is what we need on a single node
            assertThat(autoscalingCapacity.nodeSize().getBytes(), equalTo(2134900736L));
            // 7503609856 bytes = 7156MB
            // 7156MB node => 2860MB JVM heap (40% of 7156MB rounded down to a multiple of 4MB)
            // 7156MB - 2860MB - 200MB = 4096MB which is what we asked for for the tier
            assertThat(autoscalingCapacity.tierSize().getBytes(), equalTo(7503609856L));
        }
        // auto is true with unknown jvm size, memory requirement below JVM size knot point, 2 AZs (this is a realistic case for Cloud)
        {
            capacity = new NativeMemoryCapacity(
                ByteSizeValue.ofGb(4).getBytes() - 2 * NATIVE_EXECUTABLE_CODE_OVERHEAD.getBytes(),
                ByteSizeValue.ofGb(1).getBytes() - NATIVE_EXECUTABLE_CODE_OVERHEAD.getBytes()
            );
            MlMemoryAutoscalingCapacity autoscalingCapacity = capacity.autoscalingCapacity(
                randomIntBetween(5, 90),
                true,
                AUTO_ML_MEMORY_FOR_64GB_NODE,
                2
            ).build();
            // 2134900736 bytes = 2036MB
            // 2036MB node => 812MB JVM heap (40% of 2036MB rounded down to a multiple of 4MB)
            // 2036MB - 812MB - 200MB = 1024MB which is what we need on a single node
            assertThat(autoscalingCapacity.nodeSize().getBytes(), equalTo(2134900736L));
            // 7851737088 bytes = 7488MB
            // We expect to be given 2 nodes as there are 2 AZs, so each will be 3744MB
            // 3744MB node => 1496MB JVM heap (40% of 3744MB rounded down to a multiple of 4MB)
            // 3744MB - 1496MB - 200MB = 2048MB which is half of what we asked for for the tier
            // So with 2 nodes of this size we'll have the requested amount
            assertThat(autoscalingCapacity.tierSize().getBytes(), equalTo(7851737088L));
        }
        // auto is true with unknown jvm size, memory requirement below JVM size knot point, 3 AZs (this is a realistic case for Cloud)
        {
            capacity = new NativeMemoryCapacity(
                ByteSizeValue.ofGb(4).getBytes() - 3 * NATIVE_EXECUTABLE_CODE_OVERHEAD.getBytes(),
                ByteSizeValue.ofGb(1).getBytes() - NATIVE_EXECUTABLE_CODE_OVERHEAD.getBytes()
            );
            MlMemoryAutoscalingCapacity autoscalingCapacity = capacity.autoscalingCapacity(
                randomIntBetween(5, 90),
                true,
                AUTO_ML_MEMORY_FOR_64GB_NODE,
                3
            ).build();
            // 2134900736 bytes = 2036MB
            // 2036MB node => 812MB JVM heap (40% of 2036MB rounded down to a multiple of 4MB)
            // 2036MB - 812MB - 200MB = 1024MB which is what we need on a single node
            assertThat(autoscalingCapacity.nodeSize().getBytes(), equalTo(2134900736L));
            // 8195670018 bytes = 7816MB + 2 bytes
            // We expect to be given 3 nodes as there are 3 AZs, so each will be 2605 1/3MB
            // 2605 1/3MB node => 1040MB JVM heap (40% of 2605 1/3MB rounded down to a multiple of 4MB)
            // 2605 1/3MB - 1040MB - 200MB = 1365 1/3MB which is one third of what we asked for for the tier
            // So with 3 nodes of this size we'll have the requested amount
            // (The 2 byte discrepancy comes from the fact there are 3 nodes and 3 didn't divide exactly into the amount
            // of memory we needed, so each node gets a fraction of a byte extra to take it up to a whole number size)
            assertThat(autoscalingCapacity.tierSize().getBytes(), equalTo(8195670018L));
        }
        // auto is true with unknown jvm size, memory requirement below JVM size knot point, 1 AZ (this is a realistic case for Cloud)
        {
            capacity = new NativeMemoryCapacity(
                ByteSizeValue.ofGb(4).getBytes() - NATIVE_EXECUTABLE_CODE_OVERHEAD.getBytes(),
                ByteSizeValue.ofGb(3).getBytes() - NATIVE_EXECUTABLE_CODE_OVERHEAD.getBytes()
            );
            MlMemoryAutoscalingCapacity autoscalingCapacity = capacity.autoscalingCapacity(
                randomIntBetween(5, 90),
                true,
                AUTO_ML_MEMORY_FOR_64GB_NODE,
                1
            ).build();
            // 5712642048 bytes = 5448MB
            // 5448MB node => 2176MB JVM heap (40% of 5448MB rounded down to a multiple of 4MB)
            // 5448MB - 2176MB - 200MB = 3072MB which is what we need on a single node
            assertThat(autoscalingCapacity.nodeSize().getBytes(), equalTo(5712642048L));
            // 7503609856 bytes = 7156MB
            // 7156MB node => 2860MB JVM heap (40% of 7156MB rounded down to a multiple of 4MB)
            // 7156MB - 2860MB - 200MB = 4096MB which is what we asked for for the tier
            assertThat(autoscalingCapacity.tierSize().getBytes(), equalTo(7503609856L));
        }
        // auto is true with unknown jvm size, memory requirement below JVM size knot point, 2 AZs (this is a realistic case for Cloud)
        {
            capacity = new NativeMemoryCapacity(
                ByteSizeValue.ofGb(4).getBytes() - 2 * NATIVE_EXECUTABLE_CODE_OVERHEAD.getBytes(),
                ByteSizeValue.ofGb(3).getBytes() - NATIVE_EXECUTABLE_CODE_OVERHEAD.getBytes()
            );
            MlMemoryAutoscalingCapacity autoscalingCapacity = capacity.autoscalingCapacity(
                randomIntBetween(5, 90),
                true,
                AUTO_ML_MEMORY_FOR_64GB_NODE,
                2
            ).build();
            // 5712642048 bytes = 5448MB
            // 5448MB node => 2176MB JVM heap (40% of 5448MB rounded down to a multiple of 4MB)
            // 5448MB - 2176MB - 200MB = 3072MB which is what we need on a single node
            assertThat(autoscalingCapacity.nodeSize().getBytes(), equalTo(5712642048L));
            // 7851737088 bytes = 7488MB
            // We expect to be given 2 nodes as there are 2 AZs, so each will be 3744MB
            // 3744MB node => 1496MB JVM heap (40% of 3744MB rounded down to a multiple of 4MB)
            // 3744MB - 1496MB - 200MB = 2048MB which is half of what we asked for for the tier
            // So with 2 nodes of this size we'll have the requested amount
            assertThat(autoscalingCapacity.tierSize().getBytes(), equalTo(7851737088L));
        }
        // auto is true with unknown jvm size, memory requirement below JVM size knot point, 3 AZs (this is a realistic case for Cloud)
        {
            capacity = new NativeMemoryCapacity(
                ByteSizeValue.ofGb(4).getBytes() - 3 * NATIVE_EXECUTABLE_CODE_OVERHEAD.getBytes(),
                ByteSizeValue.ofGb(3).getBytes() - NATIVE_EXECUTABLE_CODE_OVERHEAD.getBytes()
            );
            MlMemoryAutoscalingCapacity autoscalingCapacity = capacity.autoscalingCapacity(
                randomIntBetween(5, 90),
                true,
                AUTO_ML_MEMORY_FOR_64GB_NODE,
                3
            ).build();
            // 5712642048 bytes = 5448MB
            // 5448MB node => 2176MB JVM heap (40% of 5448MB rounded down to a multiple of 4MB)
            // 5448MB - 2176MB - 200MB = 3072MB which is what we need on a single node
            assertThat(autoscalingCapacity.nodeSize().getBytes(), equalTo(5712642048L));
            // 8195670018 bytes = 7816MB + 2 bytes
            // We expect to be given 3 nodes as there are 3 AZs, so each will be 2605 1/3MB
            // 2605 1/3MB node => 1040MB JVM heap (40% of 2605 1/3MB rounded down to a multiple of 4MB)
            // 2605 1/3MB - 1040MB - 200MB = 1365 1/3MB which is one third of what we asked for for the tier
            // So with 3 nodes of this size we'll have the requested amount
            // (The 2 byte discrepancy comes from the fact there are 3 nodes and 3 didn't divide exactly into the amount
            // of memory we needed, so each node gets a fraction of a byte extra to take it up to a whole number size)
            assertThat(autoscalingCapacity.tierSize().getBytes(), equalTo(8195670018L));
        }
        // auto is true with unknown jvm size, memory requirement above JVM size knot point, 1 AZ (this is a realistic case for Cloud)
        {
            capacity = new NativeMemoryCapacity(
                ByteSizeValue.ofGb(30).getBytes() - NATIVE_EXECUTABLE_CODE_OVERHEAD.getBytes(),
                ByteSizeValue.ofGb(5).getBytes() - NATIVE_EXECUTABLE_CODE_OVERHEAD.getBytes()
            );
            MlMemoryAutoscalingCapacity autoscalingCapacity = capacity.autoscalingCapacity(
                randomIntBetween(5, 90),
                true,
                AUTO_ML_MEMORY_FOR_64GB_NODE,
                1
            ).build();
            // 9294577664 bytes = 8864MB
            // 8864MB node => 3544MB JVM heap (40% of 8864MB rounded down to a multiple of 4MB)
            // 8864MB - 3544MB - 200MB = 5120MB which is what we need on a single node
            assertThat(autoscalingCapacity.nodeSize().getBytes(), equalTo(9294577664L));
            // 41750102016 bytes = 39816MB
            // 39816MB node => 8896MB JVM heap (40% of 16384MB + 10% of 23432MB rounded down to a multiple of 4MB)
            // 39816MB - 8896MB - 200MB = 30720MB which is what we asked for for the tier
            assertThat(autoscalingCapacity.tierSize().getBytes(), equalTo(41750102016L));
        }
        // auto is true with unknown jvm size, memory requirement above JVM size knot point, 2 AZs (this is a realistic case for Cloud)
        {
            capacity = new NativeMemoryCapacity(
                ByteSizeValue.ofGb(30).getBytes() - 2 * NATIVE_EXECUTABLE_CODE_OVERHEAD.getBytes(),
                ByteSizeValue.ofGb(5).getBytes() - NATIVE_EXECUTABLE_CODE_OVERHEAD.getBytes()
            );
            MlMemoryAutoscalingCapacity autoscalingCapacity = capacity.autoscalingCapacity(
                randomIntBetween(5, 90),
                true,
                AUTO_ML_MEMORY_FOR_64GB_NODE,
                2
            ).build();
            // 9294577664 bytes = 8864MB
            // 8864MB node => 3544MB JVM heap (40% of 8864MB rounded down to a multiple of 4MB)
            // 8864MB - 3544MB - 200MB = 5120MB which is what we need on a single node
            assertThat(autoscalingCapacity.nodeSize().getBytes(), equalTo(9294577664L));
            // 47706013696 bytes = 45496MB
            // We expect to be given 2 nodes as there are 2 AZs, so each will be 22748MB
            // 22748MB node => 7188MB JVM heap (40% of 16384MB + 10% of 6364MB rounded down to a multiple of 4MB)
            // 22748MB - 7188MB - 200MB = 15360MB which is half of what we asked for for the tier
            // So with 2 nodes of this size we'll have the requested amount
            assertThat(autoscalingCapacity.tierSize().getBytes(), equalTo(47706013696L));
        }
        // auto is true with unknown jvm size, memory requirement above JVM size knot point, 3 AZs (this is a realistic case for Cloud)
        {
            capacity = new NativeMemoryCapacity(
                ByteSizeValue.ofGb(30).getBytes() - 3 * NATIVE_EXECUTABLE_CODE_OVERHEAD.getBytes(),
                ByteSizeValue.ofGb(5).getBytes() - NATIVE_EXECUTABLE_CODE_OVERHEAD.getBytes()
            );
            MlMemoryAutoscalingCapacity autoscalingCapacity = capacity.autoscalingCapacity(
                randomIntBetween(5, 90),
                true,
                AUTO_ML_MEMORY_FOR_64GB_NODE,
                3
            ).build();
            // 9294577664 bytes = 8864MB
            // 8864MB node => 3544MB JVM heap (40% of 8864MB rounded down to a multiple of 4MB)
            // 8864MB - 3544MB - 200MB = 5120MB which is what we need on a single node
            assertThat(autoscalingCapacity.nodeSize().getBytes(), equalTo(9294577664L));
            // 53666119680 bytes = 51180MB
            // We expect to be given 3 nodes as there are 3 AZs, so each will be 17060MB
            // 17060MB node => 6620MB JVM heap (40% of 16384MB + 10% of 676MB rounded down to a multiple of 4MB)
            // 17060MB - 6620MB - 200MB = 10240MB which is one third of what we asked for for the tier
            // So with 3 nodes of this size we'll have the requested amount
            assertThat(autoscalingCapacity.tierSize().getBytes(), equalTo(53666119680L));
        }
        // auto is true with unknown jvm size, memory requirement above JVM size knot point, 1 AZ (this is a realistic case for Cloud)
        {
            capacity = new NativeMemoryCapacity(
                ByteSizeValue.ofGb(30).getBytes() - NATIVE_EXECUTABLE_CODE_OVERHEAD.getBytes(),
                ByteSizeValue.ofGb(20).getBytes() - NATIVE_EXECUTABLE_CODE_OVERHEAD.getBytes()
            );
            MlMemoryAutoscalingCapacity autoscalingCapacity = capacity.autoscalingCapacity(
                randomIntBetween(5, 90),
                true,
                AUTO_ML_MEMORY_FOR_64GB_NODE,
                1
            ).build();
            // 29817307136 bytes = 28436MB
            // 28436MB node => 7756MB JVM heap (40% of 16384MB + 10% of 12052MB rounded down to a multiple of 4MB)
            // 28436MB - 7756MB - 200MB = 20480MB which is what we need on a single node
            assertThat(autoscalingCapacity.nodeSize().getBytes(), equalTo(29817307136L));
            // 41750102016 bytes = 39816MB
            // 39816MB node => 8896MB JVM heap (40% of 16384MB + 10% of 23432MB rounded down to a multiple of 4MB)
            // 39816MB - 8896MB - 200MB = 30720MB which is what we asked for for the tier
            assertThat(autoscalingCapacity.tierSize().getBytes(), equalTo(41750102016L));
        }
        // auto is true with unknown jvm size, memory requirement above JVM size knot point, 2 AZs (this is a realistic case for Cloud)
        {
            capacity = new NativeMemoryCapacity(
                ByteSizeValue.ofGb(30).getBytes() - 2 * NATIVE_EXECUTABLE_CODE_OVERHEAD.getBytes(),
                ByteSizeValue.ofGb(20).getBytes() - NATIVE_EXECUTABLE_CODE_OVERHEAD.getBytes()
            );
            MlMemoryAutoscalingCapacity autoscalingCapacity = capacity.autoscalingCapacity(
                randomIntBetween(5, 90),
                true,
                AUTO_ML_MEMORY_FOR_64GB_NODE,
                2
            ).build();
            // 29817307136 bytes = 28436MB
            // 28436MB node => 7756MB JVM heap (40% of 16384MB + 10% of 12052MB rounded down to a multiple of 4MB)
            // 28436MB - 7756MB - 200MB = 20480MB which is what we need on a single node
            assertThat(autoscalingCapacity.nodeSize().getBytes(), equalTo(29817307136L));
            // 47706013696 bytes = 45496MB
            // We expect to be given 2 nodes as there are 2 AZs, so each will be 22748MB
            // 22748MB node => 7188MB JVM heap (40% of 16384MB + 10% of 6364MB rounded down to a multiple of 4MB)
            // 22748MB - 7188MB - 200MB = 15360MB which is half of what we asked for for the tier
            // So with 2 nodes of this size we'll have the requested amount
            assertThat(autoscalingCapacity.tierSize().getBytes(), equalTo(47706013696L));
        }
        // auto is true with unknown jvm size, memory requirement above JVM size knot point, 3 AZs (this is a realistic case for Cloud)
        {
            capacity = new NativeMemoryCapacity(
                ByteSizeValue.ofGb(30).getBytes() - 3 * NATIVE_EXECUTABLE_CODE_OVERHEAD.getBytes(),
                ByteSizeValue.ofGb(20).getBytes() - NATIVE_EXECUTABLE_CODE_OVERHEAD.getBytes()
            );
            MlMemoryAutoscalingCapacity autoscalingCapacity = capacity.autoscalingCapacity(
                randomIntBetween(5, 90),
                true,
                AUTO_ML_MEMORY_FOR_64GB_NODE,
                3
            ).build();
            // 29817307136 bytes = 28436MB
            // 28436MB node => 7756MB JVM heap (40% of 16384MB + 10% of 12052MB rounded down to a multiple of 4MB)
            // 28436MB - 7756MB - 200MB = 20480MB which is what we need on a single node
            assertThat(autoscalingCapacity.nodeSize().getBytes(), equalTo(29817307136L));
            // 53666119680 bytes = 51180MB
            // We expect to be given 3 nodes as there are 3 AZs, so each will be 17060MB
            // 17060MB node => 6620MB JVM heap (40% of 16384MB + 10% of 676MB rounded down to a multiple of 4MB)
            // 17060MB - 6620MB - 200MB = 10240MB which is one third of what we asked for for the tier
            // So with 3 nodes of this size we'll have the requested amount
            assertThat(autoscalingCapacity.tierSize().getBytes(), equalTo(53666119680L));
        }
        // auto is true with unknown jvm size, memory requirement above single node size, 1 AZ (this is a realistic case for Cloud)
        {
            capacity = new NativeMemoryCapacity(
                ByteSizeValue.ofGb(100).getBytes() - 2 * NATIVE_EXECUTABLE_CODE_OVERHEAD.getBytes(),
                ByteSizeValue.ofGb(5).getBytes() - NATIVE_EXECUTABLE_CODE_OVERHEAD.getBytes()
            );
            MlMemoryAutoscalingCapacity autoscalingCapacity = capacity.autoscalingCapacity(
                randomIntBetween(5, 90),
                true,
                AUTO_ML_MEMORY_FOR_64GB_NODE,
                1
            ).build();
            // 9294577664 bytes = 8864MB
            // 8864MB node => 3544MB JVM heap (40% of 8864MB rounded down to a multiple of 4MB)
            // 8864MB - 3544MB - 200MB = 5120MB which is what we need on a single node
            assertThat(autoscalingCapacity.nodeSize().getBytes(), equalTo(9294577664L));
            // 131222994944 bytes = 125178MB
            // 125144MB requirement => 2 nodes needed, each 62572MB
            // 62572MB node => 11172MB JVM heap (40% of 16384MB + 10% of 46188MB rounded down to a multiple of 4MB)
            // 62572MB - 11172MB - 200MB = 51200MB which is half of what we asked for for the tier
            // So with 2 nodes of this size we'll have the requested amount
            assertThat(autoscalingCapacity.tierSize().getBytes(), equalTo(131222994944L));
        }
        // auto is true with unknown jvm size, memory requirement above single node size, 2 AZs (this is a realistic case for Cloud)
        {
            capacity = new NativeMemoryCapacity(
                ByteSizeValue.ofGb(100).getBytes() - 2 * NATIVE_EXECUTABLE_CODE_OVERHEAD.getBytes(),
                ByteSizeValue.ofGb(5).getBytes() - NATIVE_EXECUTABLE_CODE_OVERHEAD.getBytes()
            );
            MlMemoryAutoscalingCapacity autoscalingCapacity = capacity.autoscalingCapacity(
                randomIntBetween(5, 90),
                true,
                AUTO_ML_MEMORY_FOR_64GB_NODE,
                2
            ).build();
            // 9294577664 bytes = 8864MB
            // 8864MB node => 3544MB JVM heap (40% of 8864MB rounded down to a multiple of 4MB)
            // 8864MB - 3544MB - 200MB = 5120MB which is what we need on a single node
            assertThat(autoscalingCapacity.nodeSize().getBytes(), equalTo(9294577664L));
            // 131222994944 bytes = 125178MB
            // We expect to be given 2 nodes as there are 2 AZs, so each will be 62572MB
            // 62572MB node => 11172MB JVM heap (40% of 16384MB + 10% of 46188MB rounded down to a multiple of 4MB)
            // 62572MB - 11172MB - 200MB = 51200MB which is half of what we asked for for the tier
            // So with 2 nodes of this size we'll have the requested amount
            assertThat(autoscalingCapacity.tierSize().getBytes(), equalTo(131222994944L));
        }
        // auto is true with unknown jvm size, memory requirement above single node size, 3 AZs (this is a realistic case for Cloud)
        {
            capacity = new NativeMemoryCapacity(
                ByteSizeValue.ofGb(100).getBytes() - 3 * NATIVE_EXECUTABLE_CODE_OVERHEAD.getBytes(),
                ByteSizeValue.ofGb(5).getBytes() - NATIVE_EXECUTABLE_CODE_OVERHEAD.getBytes()
            );
            MlMemoryAutoscalingCapacity autoscalingCapacity = capacity.autoscalingCapacity(
                randomIntBetween(5, 90),
                true,
                AUTO_ML_MEMORY_FOR_64GB_NODE,
                3
            ).build();
            // 9294577664 bytes = 8864MB
            // 8864MB node => 3544MB JVM heap (40% of 8864MB rounded down to a multiple of 4MB)
            // 8864MB - 3544MB - 200MB = 5120MB which is what we need on a single node
            assertThat(autoscalingCapacity.nodeSize().getBytes(), equalTo(9294577664L));
            // 137170518018 bytes = 130816MB + 2 bytes
            // We expect to be given 3 nodes as there are 3 AZs, so each will be 43605 1/3MB
            // 43605 1/3MB node => 9272MB JVM heap (40% of 16384MB + 10% of 27221 1/3MB rounded down to a multiple of 4MB)
            // 43605 1/3MB - 9272MB - 200MB = 34133 1/3MB which is one third of what we asked for for the tier
            // So with 3 nodes of this size we'll have the requested amount
            // (The 2 byte discrepancy comes from the fact there are 3 nodes and 3 didn't divide exactly into the amount
            // of memory we needed, so each node gets a fraction of a byte extra to take it up to a whole number size)
            assertThat(autoscalingCapacity.tierSize().getBytes(), equalTo(137170518018L));
        }
        // auto is true with unknown jvm size, memory requirement above single node size, 1 AZ (this is a realistic case for Cloud)
        {
            capacity = new NativeMemoryCapacity(
                ByteSizeValue.ofGb(155).getBytes() - 3 * NATIVE_EXECUTABLE_CODE_OVERHEAD.getBytes(),
                ByteSizeValue.ofGb(50).getBytes() - NATIVE_EXECUTABLE_CODE_OVERHEAD.getBytes()
            );
            MlMemoryAutoscalingCapacity autoscalingCapacity = capacity.autoscalingCapacity(
                randomIntBetween(5, 90),
                true,
                AUTO_ML_MEMORY_FOR_64GB_NODE,
                1
            ).build();
            // 65611497472 bytes = 62572MB
            // 62572MB node => 11172MB JVM heap (40% of 16384MB + 10% of 46188MB rounded down to a multiple of 4MB)
            // 62572MB - 11172MB - 200MB = 51200MB which is what we need on a single node
            assertThat(autoscalingCapacity.nodeSize().getBytes(), equalTo(65611497472L));
            // 202794598401 bytes = 193400MB + 1 byte
            // 193406MB requirement => 3 nodes needed, each 64466 2/3MB
            // 64466 2/3MB node => 11360MB JVM heap (40% of 16384MB + 10% of 48082 2/3MB rounded down to a multiple of 4MB)
            // 64466 2/3MB - 11360MB - 200MB = 52906 2/3MB which is one third of what we asked for for the tier
            // So with 3 nodes of this size we'll have the requested amount
            // (The 1 byte discrepancy comes from the fact there are 3 nodes and 3 didn't divide exactly into the amount
            // of memory we needed, so each node gets a fraction of a byte extra to take it up to a whole number size)
            assertThat(autoscalingCapacity.tierSize().getBytes(), equalTo(202794598401L));
        }
        // auto is true with unknown jvm size, memory requirement above single node size, 2 AZs (this is a realistic case for Cloud)
        {
            capacity = new NativeMemoryCapacity(
                ByteSizeValue.ofGb(155).getBytes() - 4 * NATIVE_EXECUTABLE_CODE_OVERHEAD.getBytes(),
                ByteSizeValue.ofGb(50).getBytes() - NATIVE_EXECUTABLE_CODE_OVERHEAD.getBytes()
            );
            MlMemoryAutoscalingCapacity autoscalingCapacity = capacity.autoscalingCapacity(
                randomIntBetween(5, 90),
                true,
                AUTO_ML_MEMORY_FOR_64GB_NODE,
                2
            ).build();
            // 65611497472 bytes = 62572MB
            // 62572MB node => 11172MB JVM heap (40% of 16384MB + 10% of 46188MB rounded down to a multiple of 4MB)
            // 62572MB - 11172MB - 200MB = 51200MB which is what we need on a single node
            assertThat(autoscalingCapacity.nodeSize().getBytes(), equalTo(65611497472L));
            // 208758898688 bytes = 199088MB
            // We expect to be given a multiple of 2 nodes as there are 2 AZs
            // 199088MB requirement => 4 nodes needed, each 49772MB
            // 49772MB node => 9892MB JVM heap (40% of 16384MB + 10% of 33388MB rounded down to a multiple of 4MB)
            // 49772MB - 9892MB - 200MB = 39680MB which is one quarter of what we asked for for the tier
            // So with 4 nodes of this size we'll have the requested amount
            assertThat(autoscalingCapacity.tierSize().getBytes(), equalTo(208758898688L));
        }
        // auto is true with unknown jvm size, memory requirement above single node size, 3 AZs (this is a realistic case for Cloud)
        {
            capacity = new NativeMemoryCapacity(
                ByteSizeValue.ofGb(155).getBytes() - 3 * NATIVE_EXECUTABLE_CODE_OVERHEAD.getBytes(),
                ByteSizeValue.ofGb(50).getBytes() - NATIVE_EXECUTABLE_CODE_OVERHEAD.getBytes()
            );
            MlMemoryAutoscalingCapacity autoscalingCapacity = capacity.autoscalingCapacity(
                randomIntBetween(5, 90),
                true,
                AUTO_ML_MEMORY_FOR_64GB_NODE,
                3
            ).build();
            // 65611497472 bytes = 62572MB
            // 62572MB node => 11172MB JVM heap (40% of 16384MB + 10% of 46188MB rounded down to a multiple of 4MB)
            // 62572MB - 11172MB - 200MB = 51200MB which is what we need on a single node
            assertThat(autoscalingCapacity.nodeSize().getBytes(), equalTo(65611497472L));
            // 202794598401 bytes = 193400MB + 1 byte
            // We expect to be given 3 nodes as there are 3 AZs, so each will be 64466 2/3MB
            // 64466 2/3MB node => 11360MB JVM heap (40% of 16384MB + 10% of 48082 2/3MB rounded down to a multiple of 4MB)
            // 64466 2/3MB - 11360MB - 200MB = 52906 2/3MB which is one third of what we asked for for the tier
            // So with 3 nodes of this size we'll have the requested amount
            // (The 1 byte discrepancy comes from the fact there are 3 nodes and 3 didn't divide exactly into the amount
            // of memory we needed, so each node gets a fraction of a byte extra to take it up to a whole number size)
            assertThat(autoscalingCapacity.tierSize().getBytes(), equalTo(202794598401L));
        }
    }

    /**
     * When a requirement cannot be satisfied by the largest possible
     * ML node (here a huge per-node/tier requirement against a small "largest node"), the capacity
     * computation could overflow and emit a node/tier memory at or near {@link Long#MAX_VALUE}. Cloud
     * serialises the capacity as a JSON number and reads it back as a {@code long} through a
     * {@code double}, so a value of {@code 2^63} or more is rejected as out of range and corrupts the
     * persisted autoscaling entity, blocking all autoscaling. The emitted sizes must therefore stay at
     * or below {@link NativeMemoryCapacity#MAX_AUTOSCALING_CAPACITY_BYTES}, whose nearest double is
     * still below {@code 2^63}.
     */
    public void testAutoscalingCapacityDoesNotOverflowSerialization() {
        // Per-node and tier requirements far larger than the largest node the cluster is allowed to
        // grow to (the 2GB "largest ML node" simulates a small xpack.ml.max_ml_node_size). Without the
        // output cap the node-size calculation saturates to Long.MAX_VALUE and the tier multiply overflows.
        NativeMemoryCapacity unsatisfiable = new NativeMemoryCapacity(Long.MAX_VALUE / 4, Long.MAX_VALUE / 4);

        for (int numMlAvailabilityZones : new int[] { 1, 2, 3 }) {
            MlMemoryAutoscalingCapacity autoscalingCapacity = unsatisfiable.autoscalingCapacity(
                5,
                false,
                ByteSizeValue.ofGb(2).getBytes(),
                numMlAvailabilityZones
            ).build();

            assertThat(autoscalingCapacity.nodeSize().getBytes(), lessThanOrEqualTo(NativeMemoryCapacity.MAX_AUTOSCALING_CAPACITY_BYTES));
            assertThat(autoscalingCapacity.tierSize().getBytes(), lessThanOrEqualTo(NativeMemoryCapacity.MAX_AUTOSCALING_CAPACITY_BYTES));
            // The emitted value must survive Cloud's long -> double -> long round-trip, i.e. its nearest
            // double stays strictly below 2^63 = (double) Long.MAX_VALUE.
            assertThat((double) autoscalingCapacity.nodeSize().getBytes(), lessThan((double) Long.MAX_VALUE));
            assertThat((double) autoscalingCapacity.tierSize().getBytes(), lessThan((double) Long.MAX_VALUE));
            // The tier is still at least as big as a single node.
            assertThat(autoscalingCapacity.tierSize().getBytes(), greaterThanOrEqualTo(autoscalingCapacity.nodeSize().getBytes()));
        }
    }

    /**
     * Mirrors the exact scenario reported in the field: a small {@code xpack.ml.max_ml_node_size}
     * (~2GB) with a single model whose memory requirement (an ELSER model, ~3.7GB) is larger than the
     * largest possible ML node can hold. The requirement is unsatisfiable, so the decider takes the
     * "node bigger than the largest possible node" branch. The emitted node/tier sizes must still be
     * bounded: Cloud reads the capacity back as a {@code long} through a {@code double}, so a value of
     * {@code 2^63} or more corrupts the persisted autoscaling entity and blocks all autoscaling. This
     * complements {@link #testAutoscalingCapacityDoesNotOverflowSerialization()}, which forces the
     * general overflow path with synthetic requirements, by exercising the specific reported inputs.
     */
    public void testAutoscalingCapacityWithModelLargerThanMaxMlNodeSize() {
        int maxMemoryPercent = 30;
        boolean useAuto = false;
        // ELSER's model memory estimate (~3.7GB), larger than the largest ML node a 2GB max_ml_node_size allows.
        long elserModelRequirement = ByteSizeValue.ofMb(3766).getBytes();
        // Single model, so the tier requirement equals the node requirement.
        NativeMemoryCapacity capacity = new NativeMemoryCapacity(elserModelRequirement, elserModelRequirement);
        // ML native memory available on the largest node a 2GB max_ml_node_size permits.
        long mlNativeMemoryForLargestMlNode = NativeMemoryCalculator.allowedBytesForMl(
            ByteSizeValue.ofMb(2048).getBytes(),
            maxMemoryPercent,
            useAuto
        );

        for (int numMlAvailabilityZones : new int[] { 1, 2, 3 }) {
            MlMemoryAutoscalingCapacity autoscalingCapacity = capacity.autoscalingCapacity(
                maxMemoryPercent,
                useAuto,
                mlNativeMemoryForLargestMlNode,
                numMlAvailabilityZones
            ).build();

            assertThat(autoscalingCapacity.nodeSize().getBytes(), lessThanOrEqualTo(NativeMemoryCapacity.MAX_AUTOSCALING_CAPACITY_BYTES));
            assertThat(autoscalingCapacity.tierSize().getBytes(), lessThanOrEqualTo(NativeMemoryCapacity.MAX_AUTOSCALING_CAPACITY_BYTES));
            // The emitted value must survive Cloud's long -> double -> long round-trip, i.e. its nearest
            // double stays strictly below 2^63 = (double) Long.MAX_VALUE.
            assertThat((double) autoscalingCapacity.nodeSize().getBytes(), lessThan((double) Long.MAX_VALUE));
            assertThat((double) autoscalingCapacity.tierSize().getBytes(), lessThan((double) Long.MAX_VALUE));
            // The node is sized to hold the model even though that exceeds max_ml_node_size (the reported symptom).
            assertThat(autoscalingCapacity.nodeSize().getBytes(), greaterThanOrEqualTo(elserModelRequirement));
            // The tier is still at least as big as a single node.
            assertThat(autoscalingCapacity.tierSize().getBytes(), greaterThanOrEqualTo(autoscalingCapacity.nodeSize().getBytes()));
        }
    }

    public void testAutoscalingCapacityConsistency() {
        final BiConsumer<NativeMemoryCapacity, Integer> consistentAutoAssertions = (nativeMemory, memoryPercentage) -> {
            MlMemoryAutoscalingCapacity autoscalingCapacity = nativeMemory.autoscalingCapacity(25, true, Long.MAX_VALUE, 1).build();
            assertThat(
                autoscalingCapacity.tierSize().getBytes(),
                greaterThan(nativeMemory.getTierMlNativeMemoryRequirementExcludingOverhead())
            );
            assertThat(
                autoscalingCapacity.nodeSize().getBytes(),
                greaterThan(nativeMemory.getNodeMlNativeMemoryRequirementExcludingOverhead())
            );
            assertThat(autoscalingCapacity.tierSize().getBytes(), greaterThanOrEqualTo(autoscalingCapacity.nodeSize().getBytes()));
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

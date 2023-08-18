/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.ilm.IndexLifecycleFeatureSetUsage.ActionConfigStats;

public class ActionConfigStatsTests extends AbstractWireSerializingTestCase<ActionConfigStats> {

    @Override
    protected ActionConfigStats createTestInstance() {
        return createRandomInstance();
    }

    public static ActionConfigStats createRandomInstance() {
        ActionConfigStats.Builder builder = ActionConfigStats.builder();
        if (randomBoolean()) {
            builder.setAllocateNumberOfReplicas(randomIntBetween(0, 10000));
        }
        if (randomBoolean()) {
            builder.setForceMergeMaxNumberOfSegments(randomIntBetween(0, 10000));
        }
        if (randomBoolean()) {
            TimeValue randomAge = TimeValue.parseTimeValue(randomTimeValue(), "action_config_stats_tests");
            builder.setRolloverMaxAge(randomAge);
        }
        if (randomBoolean()) {
            builder.setRolloverMaxDocs(randomLongBetween(0, Long.MAX_VALUE));
        }
        if (randomBoolean()) {
            builder.setRolloverMaxPrimaryShardDocs(randomLongBetween(0, Long.MAX_VALUE));
        }
        if (randomBoolean()) {
            builder.setRolloverMaxPrimaryShardSize(randomByteSize());
        }
        if (randomBoolean()) {
            builder.setRolloverMaxSize(randomByteSize());
        }
        if (randomBoolean()) {
            TimeValue randomAge = TimeValue.parseTimeValue(randomTimeValue(), "action_config_stats_tests");
            builder.setRolloverMinAge(randomAge);
        }
        if (randomBoolean()) {
            builder.setRolloverMinDocs(randomLongBetween(0, Long.MAX_VALUE));
        }
        if (randomBoolean()) {
            builder.setRolloverMinPrimaryShardDocs(randomLongBetween(0, Long.MAX_VALUE));
        }
        if (randomBoolean()) {
            builder.setRolloverMinPrimaryShardSize(randomByteSize());
        }
        if (randomBoolean()) {
            builder.setRolloverMinSize(randomByteSize());
        }
        if (randomBoolean()) {
            builder.setPriority(randomIntBetween(0, 50));
        }
        if (randomBoolean()) {
            builder.setShrinkMaxPrimaryShardSize(randomByteSize());
        }
        if (randomBoolean()) {
            builder.setShrinkNumberOfShards(randomIntBetween(0, 50));
        }
        return builder.build();
    }

    @Override
    protected Writeable.Reader<ActionConfigStats> instanceReader() {
        return ActionConfigStats::new;
    }

    @Override
    protected ActionConfigStats mutateInstance(ActionConfigStats instance) {
        ActionConfigStats.Builder builder = ActionConfigStats.builder(instance);
        switch (between(0, 14)) {
            case 0 -> {
                int numberOfReplicas = randomValueOtherThan(instance.getAllocateNumberOfReplicas(), () -> randomIntBetween(0, 10000));
                builder.setAllocateNumberOfReplicas(numberOfReplicas);
            }
            case 1 -> {
                int numberOfSegments = randomValueOtherThan(instance.getForceMergeMaxNumberOfSegments(), () -> randomIntBetween(0, 10000));
                builder.setForceMergeMaxNumberOfSegments(numberOfSegments);
            }
            case 2 -> {
                TimeValue randomAge = randomValueOtherThan(
                    instance.getRolloverMaxAge(),
                    () -> TimeValue.parseTimeValue(randomTimeValue(), "action_config_stats_tests")
                );
                builder.setRolloverMaxAge(randomAge);
            }
            case 3 -> builder.setRolloverMaxDocs(randomLongBetween(0, Long.MAX_VALUE));
            case 4 -> builder.setRolloverMaxPrimaryShardDocs(randomLongBetween(0, Long.MAX_VALUE));
            case 5 -> {
                builder.setRolloverMaxPrimaryShardSize(randomByteSize());
            }
            case 6 -> {
                builder.setRolloverMaxSize(randomByteSize());
            }
            case 7 -> {
                TimeValue randomAge = randomValueOtherThan(
                    instance.getRolloverMinAge(),
                    () -> TimeValue.parseTimeValue(randomTimeValue(), "action_config_stats_tests")
                );
                builder.setRolloverMinAge(randomAge);
            }
            case 8 -> builder.setRolloverMinDocs(randomLongBetween(0, Long.MAX_VALUE));
            case 9 -> builder.setRolloverMinPrimaryShardDocs(randomLongBetween(0, Long.MAX_VALUE));
            case 10 -> {
                builder.setRolloverMinPrimaryShardSize(randomByteSize());
            }
            case 11 -> {
                builder.setRolloverMinSize(randomByteSize());
            }
            case 12 -> builder.setPriority(randomValueOtherThan(instance.getSetPriorityPriority(), () -> randomIntBetween(0, 50)));
            case 13 -> {
                builder.setShrinkMaxPrimaryShardSize(randomByteSize());
            }
            case 14 -> builder.setShrinkNumberOfShards(
                randomValueOtherThan(instance.getShrinkNumberOfShards(), () -> randomIntBetween(0, 50))
            );
            default -> throw new IllegalStateException("Illegal randomization branch");
        }
        return builder.build();
    }

    private static ByteSizeValue randomByteSize() {
        return ByteSizeValue.ofBytes(randomLongBetween(0, 1024L * 1024L * 1024L * 50L));
    }
}

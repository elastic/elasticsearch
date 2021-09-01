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

import java.io.IOException;

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
            ByteSizeValue randomByteSize = ByteSizeValue.ofBytes(randomLongBetween(0, 1024L*1024L*1024L*50L));
            builder.setRolloverMaxPrimaryShardSize(randomByteSize);
        }
        if (randomBoolean()) {
            ByteSizeValue randomByteSize = ByteSizeValue.ofBytes(randomLongBetween(0, 1024L*1024L*1024L*50L));
            builder.setRolloverMaxSize(randomByteSize);
        }
        if (randomBoolean()) {
            builder.setPriority(randomIntBetween(0, 50));
        }
        if (randomBoolean()) {
            ByteSizeValue randomByteSize = ByteSizeValue.ofBytes(randomLongBetween(0, 1024L*1024L*1024L*50L));
            builder.setShrinkMaxPrimaryShardSize(randomByteSize);
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
    protected ActionConfigStats mutateInstance(ActionConfigStats instance) throws IOException {
        ActionConfigStats.Builder builder = ActionConfigStats.builder(instance);
        switch (between(0, 8)) {
            case 0:
                int numberOfReplicas = randomValueOtherThan(instance.getAllocateNumberOfReplicas(), () -> randomIntBetween(0, 10000));
                builder.setAllocateNumberOfReplicas(numberOfReplicas);
                break;
            case 1:
                int numberOfSegments = randomValueOtherThan(instance.getForceMergeMaxNumberOfSegments(), () -> randomIntBetween(0, 10000));
                builder.setForceMergeMaxNumberOfSegments(numberOfSegments);
                break;
            case 2:
                TimeValue randomAge = randomValueOtherThan(instance.getRolloverMaxAge(),
                    () -> TimeValue.parseTimeValue(randomTimeValue(), "action_config_stats_tests"));
                builder.setRolloverMaxAge(randomAge);
                break;
            case 3:
                builder.setRolloverMaxDocs(randomLongBetween(0, Long.MAX_VALUE));
                break;
            case 4:
                ByteSizeValue randomByteSize = ByteSizeValue.ofBytes(randomLongBetween(0, 1024L*1024L*1024L*50L));
                builder.setRolloverMaxPrimaryShardSize(randomByteSize);
                break;
            case 5:
                ByteSizeValue randomMaxByteSize = ByteSizeValue.ofBytes(randomLongBetween(0, 1024L*1024L*1024L*50L));
                builder.setRolloverMaxSize(randomMaxByteSize);
                break;
            case 6:
                builder.setPriority(randomValueOtherThan(instance.getSetPriorityPriority(), () -> randomIntBetween(0, 50)));
                break;
            case 7:
                ByteSizeValue randomPrimaryByteSize = ByteSizeValue.ofBytes(randomLongBetween(0, 1024L*1024L*1024L*50L));
                builder.setShrinkMaxPrimaryShardSize(randomPrimaryByteSize);
                break;
            case 8:
                builder.setShrinkNumberOfShards(randomValueOtherThan(instance.getShrinkNumberOfShards(), () -> randomIntBetween(0, 50)));
                break;
            default:
                throw new IllegalStateException("Illegal randomization branch");
        }
        return builder.build();
    }
}

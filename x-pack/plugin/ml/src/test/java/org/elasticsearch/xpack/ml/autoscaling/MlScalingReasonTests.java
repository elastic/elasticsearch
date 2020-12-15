/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.autoscaling;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.autoscaling.capacity.AutoscalingCapacity;

import java.util.stream.Collectors;
import java.util.stream.Stream;

public class MlScalingReasonTests extends AbstractWireSerializingTestCase<MlScalingReason> {

    @Override
    protected Writeable.Reader<MlScalingReason> instanceReader() {
        return MlScalingReason::new;
    }

    @Override
    protected MlScalingReason createTestInstance() {
        return new MlScalingReason(
            randomBoolean() ? null : Stream.generate(() -> randomAlphaOfLength(10)).limit(5).collect(Collectors.toList()),
            randomBoolean() ? null : Stream.generate(() -> randomAlphaOfLength(10)).limit(5).collect(Collectors.toList()),
            randomConfiguration(),
            randomBoolean() ? null : randomLongBetween(10, ByteSizeValue.ofGb(1).getBytes()),
            randomBoolean() ? null : randomLongBetween(10, ByteSizeValue.ofGb(1).getBytes()),
            new AutoscalingCapacity(AutoscalingCapacity.AutoscalingResources.ZERO, AutoscalingCapacity.AutoscalingResources.ZERO),
            randomAlphaOfLength(10)
            );
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(MlAutoscalingNamedWritableProvider.getNamedWriteables());
    }

    private static Settings randomConfiguration() {
        Settings.Builder builder = Settings.builder();
        if (randomBoolean()) {
            builder.put(MlAutoscalingDeciderService.NUM_ANALYTICS_JOBS_IN_QUEUE.getKey(), randomIntBetween(0, 10));
        }
        if (randomBoolean()) {
            builder.put(MlAutoscalingDeciderService.NUM_ANOMALY_JOBS_IN_QUEUE.getKey(), randomIntBetween(0, 10));
        }
        if (randomBoolean()) {
            builder.put(MlAutoscalingDeciderService.DOWN_SCALE_DELAY.getKey(), randomTimeValue());
        }
        return builder.build();
    }

}

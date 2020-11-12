/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.autoscaling;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.xpack.autoscaling.capacity.AutoscalingDeciderConfiguration;
import org.elasticsearch.xpack.autoscaling.capacity.AutoscalingDeciderResult;

import java.util.Arrays;
import java.util.List;

public final class MlAutoscalingNamedWritableProvider {

    private MlAutoscalingNamedWritableProvider() { }

    public static List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        return Arrays.asList(
            new NamedWriteableRegistry.Entry(AutoscalingDeciderConfiguration.class,
                MlAutoscalingDeciderConfiguration.NAME,
                MlAutoscalingDeciderConfiguration::new),
            new NamedWriteableRegistry.Entry(AutoscalingDeciderResult.Reason.class,
                MlScalingReason.NAME,
                MlScalingReason::new)
        );
    }
}

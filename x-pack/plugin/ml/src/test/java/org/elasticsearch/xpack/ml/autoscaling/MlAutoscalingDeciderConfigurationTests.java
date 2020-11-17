/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.autoscaling;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;


public class MlAutoscalingDeciderConfigurationTests extends AbstractSerializingTestCase<MlAutoscalingDeciderConfiguration> {

    public static MlAutoscalingDeciderConfiguration randomInstance() {
        MlAutoscalingDeciderConfiguration.Builder builder = MlAutoscalingDeciderConfiguration.builder();
        if (randomBoolean()) {
            builder.setNumAnalyticsJobsInQueue(randomIntBetween(0, 10));
        }
        if (randomBoolean()) {
            builder.setNumAnomalyJobsInQueue(randomIntBetween(0, 10));
        }
        if (randomBoolean()) {
            builder.setDownScaleDelay(randomTimeValue());
        }
        return builder.build();
    }

    @Override
    protected MlAutoscalingDeciderConfiguration doParseInstance(XContentParser parser) {
        return MlAutoscalingDeciderConfiguration.parse(parser);
    }

    @Override
    protected Writeable.Reader<MlAutoscalingDeciderConfiguration> instanceReader() {
        return MlAutoscalingDeciderConfiguration::new;
    }

    @Override
    protected MlAutoscalingDeciderConfiguration createTestInstance() {
        return randomInstance();
    }
}

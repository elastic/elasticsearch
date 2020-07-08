/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.autoscaling;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;

import java.io.IOException;

public class MlAutoscalingDeciderConfigurationTests extends AbstractSerializingTestCase<MlAutoscalingDeciderConfiguration> {
    @Override
    protected MlAutoscalingDeciderConfiguration doParseInstance(XContentParser parser) throws IOException {
        return MlAutoscalingDeciderConfiguration.parse(parser);
    }

    @Override
    protected Writeable.Reader<MlAutoscalingDeciderConfiguration> instanceReader() {
        return MlAutoscalingDeciderConfiguration::new;
    }

    @Override
    protected MlAutoscalingDeciderConfiguration createTestInstance() {
        MlAutoscalingDeciderConfiguration.Builder builder = MlAutoscalingDeciderConfiguration.builder();
        if (randomBoolean()) {
            builder.setAnalysisJobTimeInQueue(randomTimeValue());
        }
        if (randomBoolean()) {
            builder.setAnomalyJobTimeInQueue(randomTimeValue());
        }
        if (randomBoolean()) {
            builder.setMinNumNodes(randomIntBetween(0, 100));
        }
        return builder.build();
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference;

import com.carrotsearch.randomizedtesting.generators.RandomStrings;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;

public class InferenceFeatureSetUsageTests extends AbstractWireSerializingTestCase<InferenceFeatureSetUsage.EndpointStats> {

    @Override
    protected Writeable.Reader<InferenceFeatureSetUsage.EndpointStats> instanceReader() {
        return InferenceFeatureSetUsage.EndpointStats::new;
    }

    @Override
    protected InferenceFeatureSetUsage.EndpointStats createTestInstance() {
        RandomStrings.randomAsciiLettersOfLength(random(), 10);
        return new InferenceFeatureSetUsage.EndpointStats(
            randomIdentifier(),
            TaskType.values()[randomInt(TaskType.values().length - 1)],
            randomInt(10)
        );
    }

    @Override
    protected InferenceFeatureSetUsage.EndpointStats mutateInstance(InferenceFeatureSetUsage.EndpointStats modelStats) throws IOException {
        InferenceFeatureSetUsage.EndpointStats newModelStats = new InferenceFeatureSetUsage.EndpointStats(modelStats);
        newModelStats.add();
        return newModelStats;
    }
}

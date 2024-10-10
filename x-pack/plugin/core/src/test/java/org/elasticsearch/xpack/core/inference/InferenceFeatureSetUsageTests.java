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

public class InferenceFeatureSetUsageTests extends AbstractWireSerializingTestCase<InferenceFeatureSetUsage.ModelStats> {

    @Override
    protected Writeable.Reader<InferenceFeatureSetUsage.ModelStats> instanceReader() {
        return InferenceFeatureSetUsage.ModelStats::new;
    }

    @Override
    protected InferenceFeatureSetUsage.ModelStats createTestInstance() {
        RandomStrings.randomAsciiLettersOfLength(random(), 10);
        return new InferenceFeatureSetUsage.ModelStats(
            randomIdentifier(),
            TaskType.values()[randomInt(TaskType.values().length - 1)],
            randomInt(10)
        );
    }

    @Override
    protected InferenceFeatureSetUsage.ModelStats mutateInstance(InferenceFeatureSetUsage.ModelStats modelStats) throws IOException {
        InferenceFeatureSetUsage.ModelStats newModelStats = new InferenceFeatureSetUsage.ModelStats(modelStats);
        newModelStats.add();
        return newModelStats;
    }
}

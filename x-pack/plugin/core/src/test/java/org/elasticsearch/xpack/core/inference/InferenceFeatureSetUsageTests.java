/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.inference.usage.ModelStats;
import org.elasticsearch.xpack.core.inference.usage.ModelStatsTests;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class InferenceFeatureSetUsageTests extends AbstractWireSerializingTestCase<InferenceFeatureSetUsage> {

    @Override
    protected Writeable.Reader<InferenceFeatureSetUsage> instanceReader() {
        return InferenceFeatureSetUsage::new;
    }

    @Override
    protected InferenceFeatureSetUsage createTestInstance() {
        return new InferenceFeatureSetUsage(randomList(10, ModelStatsTests::createRandomInstance));
    }

    @Override
    protected InferenceFeatureSetUsage mutateInstance(InferenceFeatureSetUsage instance) throws IOException {
        List<ModelStats> mutatedModelStats = new ArrayList<>(instance.modelStats());
        if (mutatedModelStats.isEmpty()) {
            mutatedModelStats.add(ModelStatsTests.createRandomInstance());
        } else {
            mutatedModelStats.remove(randomIntBetween(0, mutatedModelStats.size() - 1));
        }
        return new InferenceFeatureSetUsage(mutatedModelStats);
    }
}

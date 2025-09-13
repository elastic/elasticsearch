/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.usage;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;

public class ModelStatsTests extends AbstractWireSerializingTestCase<ModelStats> {

    @Override
    protected Writeable.Reader<ModelStats> instanceReader() {
        return ModelStats::new;
    }

    @Override
    protected ModelStats createTestInstance() {
        return createRandomInstance();
    }

    @Override
    protected ModelStats mutateInstance(ModelStats modelStats) throws IOException {
        ModelStats newModelStats = new ModelStats(modelStats);
        newModelStats.add();
        return newModelStats;
    }

    public static ModelStats createRandomInstance() {
        return new ModelStats(randomIdentifier(), TaskType.values()[randomInt(TaskType.values().length - 1)], randomInt(10));
    }
}

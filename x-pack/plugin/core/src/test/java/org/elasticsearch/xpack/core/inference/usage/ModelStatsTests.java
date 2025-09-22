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
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

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
        String service = modelStats.service();
        TaskType taskType = modelStats.taskType();
        long count = modelStats.count();
        return switch (randomInt(2)) {
            case 0 -> new ModelStats(randomValueOtherThan(service, ESTestCase::randomIdentifier), taskType, count);
            case 1 -> new ModelStats(service, randomValueOtherThan(taskType, () -> randomFrom(TaskType.values())), count);
            case 2 -> new ModelStats(service, taskType, randomValueOtherThan(count, ESTestCase::randomLong));
            default -> throw new IllegalArgumentException();
        };
    }

    public void testAdd() {
        ModelStats stats = new ModelStats("test_service", randomFrom(TaskType.values()));
        assertThat(stats.count(), equalTo(0L));

        stats.add();
        assertThat(stats.count(), equalTo(1L));

        int iterations = randomIntBetween(1, 10);
        for (int i = 0; i < iterations; i++) {
            stats.add();
        }
        assertThat(stats.count(), equalTo(1L + iterations));
    }

    public static ModelStats createRandomInstance() {
        return new ModelStats(randomIdentifier(), randomFrom(TaskType.values()), randomLong());
    }
}

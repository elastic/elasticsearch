/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.usage;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class ModelStatsTests extends AbstractBWCWireSerializationTestCase<ModelStats> {

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
        SemanticTextStats semanticTextStats = modelStats.semanticTextStats();
        return switch (randomInt(3)) {
            case 0 -> new ModelStats(randomValueOtherThan(service, ESTestCase::randomIdentifier), taskType, count, semanticTextStats);
            case 1 -> new ModelStats(
                service,
                randomValueOtherThan(taskType, () -> randomFrom(TaskType.values())),
                count,
                semanticTextStats
            );
            case 2 -> new ModelStats(service, taskType, randomValueOtherThan(count, ESTestCase::randomLong), semanticTextStats);
            case 3 -> new ModelStats(
                service,
                taskType,
                count,
                randomValueOtherThan(semanticTextStats, SemanticTextStatsTests::createRandomInstance)
            );
            default -> throw new IllegalArgumentException();
        };
    }

    public void testAdd() {
        ModelStats stats = new ModelStats("test_service", randomFrom(TaskType.values()), 0, null);
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
        TaskType taskType = randomValueOtherThan(TaskType.ANY, () -> randomFrom(TaskType.values()));
        return new ModelStats(
            randomIdentifier(),
            taskType,
            randomLong(),
            randomBoolean() ? SemanticTextStatsTests.createRandomInstance() : null
        );
    }

    @Override
    protected ModelStats mutateInstanceForVersion(ModelStats instance, TransportVersion version) {
        if (version.supports(ModelStats.INFERENCE_TELEMETRY_ADDED_SEMANTIC_TEXT_STATS) == false) {
            return new ModelStats(instance.service(), instance.taskType(), instance.count(), null);
        }
        return instance;
    }
}

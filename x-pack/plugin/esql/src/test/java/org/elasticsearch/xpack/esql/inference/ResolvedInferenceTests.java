/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.AbstractWireTestCase;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

public class ResolvedInferenceTests extends AbstractWireTestCase<ResolvedInference> {

    @Override
    protected ResolvedInference createTestInstance() {
        return new ResolvedInference(randomIdentifier(), randomTaskType(), randomBoolean() ? null : randomFrom(SimilarityMeasure.values()));
    }

    @Override
    protected ResolvedInference mutateInstance(ResolvedInference instance) throws IOException {
        if (randomBoolean()) {
            return new ResolvedInference(randomValueOtherThan(instance.inferenceId(), ESTestCase::randomIdentifier), instance.taskType());
        }
        if (randomBoolean()) {
            return new ResolvedInference(
                instance.inferenceId(),
                randomValueOtherThan(instance.taskType(), this::randomTaskType),
                instance.similarity()
            );
        }
        return new ResolvedInference(
            instance.inferenceId(),
            instance.taskType(),
            randomValueOtherThan(instance.similarity(), () -> randomFrom(SimilarityMeasure.values()))
        );
    }

    @Override
    protected ResolvedInference copyInstance(ResolvedInference instance, TransportVersion version) throws IOException {
        return copyInstance(instance, getNamedWriteableRegistry(), (out, v) -> v.writeTo(out), in -> new ResolvedInference(in), version);
    }

    private TaskType randomTaskType() {
        return randomFrom(TaskType.values());
    }
}

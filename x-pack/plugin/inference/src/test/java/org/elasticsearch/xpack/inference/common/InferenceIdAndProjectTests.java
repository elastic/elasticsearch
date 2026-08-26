/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.common;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;

import java.io.IOException;

public class InferenceIdAndProjectTests extends AbstractBWCWireSerializationTestCase<InferenceIdAndProject> {

    @Override
    protected Writeable.Reader<InferenceIdAndProject> instanceReader() {
        return InferenceIdAndProject::new;
    }

    @Override
    protected InferenceIdAndProject createTestInstance() {
        return randomInstance();
    }

    @Override
    protected InferenceIdAndProject mutateInstance(InferenceIdAndProject instance) throws IOException {
        var inferenceEntityId = instance.inferenceEntityId();
        var projectId = instance.projectId();
        switch (randomInt(1)) {
            case 0 -> inferenceEntityId = randomValueOtherThan(inferenceEntityId, () -> randomAlphaOfLength(10));
            case 1 -> projectId = randomValueOtherThan(projectId, () -> ProjectId.fromId(randomAlphaOfLength(8)));
            default -> throw new IllegalStateException("Illegal randomization switch case");
        }

        return new InferenceIdAndProject(inferenceEntityId, projectId);
    }

    @Override
    protected InferenceIdAndProject mutateInstanceForVersion(InferenceIdAndProject instance, TransportVersion version) {
        return instance;
    }

    public static InferenceIdAndProject randomInstance() {
        var projectId = randomBoolean() ? ProjectId.DEFAULT : ProjectId.fromId(randomAlphaOfLength(8));
        return new InferenceIdAndProject(randomAlphaOfLength(10), projectId);
    }
}

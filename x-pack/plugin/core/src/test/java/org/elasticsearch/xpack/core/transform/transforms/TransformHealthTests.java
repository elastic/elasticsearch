/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform.transforms;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.health.HealthStatus;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

public class TransformHealthTests extends AbstractWireSerializingTestCase<TransformHealth> {

    public static TransformHealth randomTransformHealth() {
        return new TransformHealth(
            randomFrom(HealthStatus.values()),
            randomBoolean() ? null : randomList(1, 10, TransformHealthIssueTests::randomTransformHealthIssue)
        );
    }

    @Override
    protected Writeable.Reader<TransformHealth> instanceReader() {
        return TransformHealth::new;
    }

    @Override
    protected TransformHealth createTestInstance() {
        return randomTransformHealth();
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform.transforms;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.function.Predicate;

public class TransformStatsTests extends AbstractXContentSerializingTestCase<TransformStats> {

    public static TransformStats randomTransformStats() {
        return new TransformStats(
            randomAlphaOfLength(10),
            randomFrom(TransformStats.State.values()),
            randomBoolean() ? null : randomAlphaOfLength(100),
            randomBoolean() ? null : NodeAttributeTests.randomNodeAttributes(),
            TransformIndexerStatsTests.randomStats(),
            TransformCheckpointingInfoTests.randomTransformCheckpointingInfo(),
            TransformHealthTests.randomTransformHealth()
        );
    }

    @Override
    protected TransformStats doParseInstance(XContentParser parser) throws IOException {
        return TransformStats.fromXContent(parser);
    }

    @Override
    protected TransformStats createTestInstance() {
        return randomTransformStats();
    }

    @Override
    protected Reader<TransformStats> instanceReader() {
        return TransformStats::new;
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    @Override
    protected String[] getShuffleFieldsExceptions() {
        return new String[] { "position" };
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        return field -> field.isEmpty() == false;
    }

}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.transform.transforms;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;

import java.io.IOException;
import java.util.function.Predicate;

public class TransformStatsTests extends AbstractSerializingTestCase<TransformStats> {

    public static TransformStats randomDataFrameTransformStats() {
        return new TransformStats(randomAlphaOfLength(10),
            randomFrom(TransformStats.State.values()),
            randomBoolean() ? null : randomAlphaOfLength(100),
            randomBoolean() ? null : NodeAttributeTests.randomNodeAttributes(),
            TransformIndexerStatsTests.randomStats(),
            TransformCheckpointingInfoTests.randomDataFrameTransformCheckpointingInfo());
    }

    @Override
    protected TransformStats doParseInstance(XContentParser parser) throws IOException {
        return TransformStats.fromXContent(parser);
    }

    @Override
    protected TransformStats createTestInstance() {
        return randomDataFrameTransformStats();
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
        return field -> !field.isEmpty();
    }
}

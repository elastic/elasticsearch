/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.transform.transforms;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xpack.core.indexing.IndexerState;

import java.io.IOException;
import java.util.function.Predicate;

import static org.elasticsearch.xpack.core.transform.transforms.TransformProgressTests.randomTransformProgress;
import static org.elasticsearch.xpack.core.transform.transforms.NodeAttributeTests.randomNodeAttributes;

public class TransformStateTests extends AbstractSerializingTestCase<TransformState> {

    public static TransformState randomTransformState() {
        return new TransformState(
            randomFrom(TransformTaskState.values()),
            randomFrom(IndexerState.values()),
            TransformIndexerPositionTests.randomTransformIndexerPosition(),
            randomLongBetween(0, 10),
            randomBoolean() ? null : randomAlphaOfLength(10),
            randomBoolean() ? null : randomTransformProgress(),
            randomBoolean() ? null : randomNodeAttributes(),
            randomBoolean()
        );
    }

    @Override
    protected TransformState doParseInstance(XContentParser parser) throws IOException {
        return TransformState.fromXContent(parser);
    }

    @Override
    protected TransformState createTestInstance() {
        return randomTransformState();
    }

    @Override
    protected Reader<TransformState> instanceReader() {
        return TransformState::new;
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        return field -> !field.isEmpty();
    }

    public void testBackwardsSerialization() throws IOException {
        TransformState state = new TransformState(
            randomFrom(TransformTaskState.values()),
            randomFrom(IndexerState.values()),
            TransformIndexerPositionTests.randomTransformIndexerPosition(),
            randomLongBetween(0, 10),
            randomBoolean() ? null : randomAlphaOfLength(10),
            randomBoolean() ? null : randomTransformProgress(),
            randomBoolean() ? null : randomNodeAttributes(),
            false
        ); // Will be false after BWC deserialization
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            output.setVersion(Version.V_7_5_0);
            state.writeTo(output);
            try (StreamInput in = output.bytes().streamInput()) {
                in.setVersion(Version.V_7_5_0);
                TransformState streamedState = new TransformState(in);
                assertEquals(state, streamedState);
            }
        }
    }
}

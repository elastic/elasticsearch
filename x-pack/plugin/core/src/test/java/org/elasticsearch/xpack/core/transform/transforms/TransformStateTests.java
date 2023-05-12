/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform.transforms;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.indexing.IndexerState;

import java.io.IOException;
import java.util.function.Predicate;

import static org.elasticsearch.xpack.core.transform.transforms.NodeAttributeTests.randomNodeAttributes;
import static org.elasticsearch.xpack.core.transform.transforms.TransformProgressTests.randomTransformProgress;

public class TransformStateTests extends AbstractXContentSerializingTestCase<TransformState> {

    public static TransformState randomTransformState() {
        return new TransformState(
            randomFrom(TransformTaskState.values()),
            randomFrom(IndexerState.values()),
            TransformIndexerPositionTests.randomTransformIndexerPosition(),
            randomLongBetween(0, 10),
            randomBoolean() ? null : randomAlphaOfLength(10),
            randomBoolean() ? null : randomTransformProgress(),
            randomBoolean() ? null : randomNodeAttributes(),
            randomBoolean(),
            randomBoolean() ? null : AuthorizationStateTests.randomAuthorizationState()
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
    protected TransformState mutateInstance(TransformState instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
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
        return field -> field.isEmpty() == false;
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
            false,
            randomBoolean() ? null : AuthorizationStateTests.randomAuthorizationState()
        );
        // auth_state will be null after BWC deserialization
        TransformState expectedState = new TransformState(
            state.getTaskState(),
            state.getIndexerState(),
            state.getPosition(),
            state.getCheckpoint(),
            state.getReason(),
            state.getProgress(),
            state.getNode(),
            state.shouldStopAtNextCheckpoint(),
            null
        );
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            output.setTransportVersion(TransportVersion.V_7_5_0);
            state.writeTo(output);
            try (StreamInput in = output.bytes().streamInput()) {
                in.setTransportVersion(TransportVersion.V_7_5_0);
                TransformState streamedState = new TransformState(in);
                assertEquals(expectedState, streamedState);
            }
        }
    }
}

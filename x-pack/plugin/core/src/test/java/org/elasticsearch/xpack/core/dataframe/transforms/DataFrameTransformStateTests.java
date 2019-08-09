/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.dataframe.transforms;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xpack.core.indexing.IndexerState;

import java.io.IOException;
import java.util.function.Predicate;

import static org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformProgressTests.randomDataFrameTransformProgress;
import static org.elasticsearch.xpack.core.dataframe.transforms.NodeAttributeTests.randomNodeAttributes;

public class DataFrameTransformStateTests extends AbstractSerializingTestCase<DataFrameTransformState> {

    public static DataFrameTransformState randomDataFrameTransformState() {
        return new DataFrameTransformState(randomFrom(DataFrameTransformTaskState.values()),
            randomFrom(IndexerState.values()),
            DataFrameIndexerPositionTests.randomDataFrameIndexerPosition(),
            randomLongBetween(0,10),
            randomBoolean() ? null : randomAlphaOfLength(10),
            randomBoolean() ? null : randomDataFrameTransformProgress(),
            randomBoolean() ? null : randomNodeAttributes(),
            randomBoolean());
    }

    @Override
    protected DataFrameTransformState doParseInstance(XContentParser parser) throws IOException {
        return DataFrameTransformState.fromXContent(parser);
    }

    @Override
    protected DataFrameTransformState createTestInstance() {
        return randomDataFrameTransformState();
    }

    @Override
    protected Reader<DataFrameTransformState> instanceReader() {
        return DataFrameTransformState::new;
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
        DataFrameTransformState state = new DataFrameTransformState(randomFrom(DataFrameTransformTaskState.values()),
            randomFrom(IndexerState.values()),
            DataFrameIndexerPositionTests.randomDataFrameIndexerPosition(),
            randomLongBetween(0,10),
            randomBoolean() ? null : randomAlphaOfLength(10),
            randomBoolean() ? null : randomDataFrameTransformProgress(),
            randomBoolean() ? null : randomNodeAttributes(),
            false); // Will be false after BWC deserialization
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            output.setVersion(Version.V_7_4_0);
            state.writeTo(output);
            try (StreamInput in = output.bytes().streamInput()) {
                in.setVersion(Version.V_7_4_0);
                DataFrameTransformState streamedState = new DataFrameTransformState(in);
                assertEquals(state, streamedState);
            }
        }
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.dataframe.transforms;

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
            randomBoolean() ? null : randomNodeAttributes());
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
}

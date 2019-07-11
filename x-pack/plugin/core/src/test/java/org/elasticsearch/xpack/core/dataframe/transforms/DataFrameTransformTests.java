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
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class DataFrameTransformTests extends AbstractSerializingDataFrameTestCase<DataFrameTransform> {

    @Override
    protected DataFrameTransform doParseInstance(XContentParser parser) throws IOException {
        return DataFrameTransform.PARSER.apply(parser, null);
    }

    @Override
    protected DataFrameTransform createTestInstance() {
        return new DataFrameTransform(randomAlphaOfLength(10), randomBoolean() ? null : Version.CURRENT,
            randomBoolean() ? null : TimeValue.timeValueMillis(randomIntBetween(1_000, 3_600_000)));
    }

    @Override
    protected Reader<DataFrameTransform> instanceReader() {
        return DataFrameTransform::new;
    }

    public void testBackwardsSerialization() throws IOException {
        for (int i = 0; i < NUMBER_OF_TEST_RUNS; i++) {
            DataFrameTransform transformTask = createTestInstance();
            try (BytesStreamOutput output = new BytesStreamOutput()) {
                output.setVersion(Version.V_7_2_0);
                transformTask.writeTo(output);
                try (StreamInput in = output.bytes().streamInput()) {
                    in.setVersion(Version.V_7_2_0);
                    // Since the old version does not have the version serialized, the version NOW is 7.2.0
                    DataFrameTransform streamedTask = new DataFrameTransform(in);
                    assertThat(streamedTask.getVersion(), equalTo(Version.V_7_2_0));
                    assertThat(streamedTask.getId(), equalTo(transformTask.getId()));
                }
            }
        }
    }
}

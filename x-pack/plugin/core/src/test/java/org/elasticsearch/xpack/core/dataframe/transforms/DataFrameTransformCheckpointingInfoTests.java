/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.dataframe.transforms;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;

public class DataFrameTransformCheckpointingInfoTests extends AbstractSerializingDataFrameTestCase<DataFrameTransformCheckpointingInfo> {

    public static DataFrameTransformCheckpointingInfo randomDataFrameTransformCheckpointingInfo() {
        return new DataFrameTransformCheckpointingInfo(DataFrameTransformCheckpointStatsTests.randomDataFrameTransformCheckpointStats(),
                DataFrameTransformCheckpointStatsTests.randomDataFrameTransformCheckpointStats(), randomNonNegativeLong());
    }

    @Override
    protected DataFrameTransformCheckpointingInfo doParseInstance(XContentParser parser) throws IOException {
        return DataFrameTransformCheckpointingInfo.fromXContent(parser);
    }

    @Override
    protected DataFrameTransformCheckpointingInfo createTestInstance() {
        return randomDataFrameTransformCheckpointingInfo();
    }

    @Override
    protected Reader<DataFrameTransformCheckpointingInfo> instanceReader() {
        return DataFrameTransformCheckpointingInfo::new;
    }
}

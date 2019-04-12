/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.dataframe.transforms;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.dataframe.DataFrameField;

import java.io.IOException;
import java.util.Collections;

public class DataFrameTransformStateAndStatsTests extends AbstractSerializingDataFrameTestCase<DataFrameTransformStateAndStats> {

    protected static ToXContent.Params TO_XCONTENT_PARAMS = new ToXContent.MapParams(
        Collections.singletonMap(DataFrameField.FOR_INTERNAL_STORAGE, "true"));

    public static DataFrameTransformStateAndStats randomDataFrameTransformStateAndStats(String id) {
        return new DataFrameTransformStateAndStats(id,
                DataFrameTransformStateTests.randomDataFrameTransformState(),
                DataFrameIndexerTransformStatsTests.randomStats(id),
                DataFrameTransformCheckpointingInfoTests.randomDataFrameTransformCheckpointingInfo());
    }

    public static DataFrameTransformStateAndStats randomDataFrameTransformStateAndStats() {
        return randomDataFrameTransformStateAndStats(randomAlphaOfLengthBetween(1, 10));
    }

    @Override
    protected DataFrameTransformStateAndStats doParseInstance(XContentParser parser) throws IOException {
        return DataFrameTransformStateAndStats.PARSER.apply(parser, null);
    }

    @Override
    // Setting params for internal storage so that we can check XContent equivalence as
    // DataFrameIndexerTransformStats does not write the ID to the XContentObject unless it is for internal storage
    protected ToXContent.Params getToXContentParams() {
        return TO_XCONTENT_PARAMS;
    }

    @Override
    protected DataFrameTransformStateAndStats createTestInstance() {
        return randomDataFrameTransformStateAndStats();
    }

    @Override
    protected Reader<DataFrameTransformStateAndStats> instanceReader() {
        return DataFrameTransformStateAndStats::new;
    }

}

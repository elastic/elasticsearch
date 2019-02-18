/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.dataframe.action;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.dataframe.transform.DataFrameIndexerTransformStatsTests;
import org.elasticsearch.xpack.core.dataframe.transform.DataFrameTransformStateTests;
import org.elasticsearch.xpack.dataframe.transforms.AbstractSerializingDataFrameTestCase;

import java.io.IOException;

public class DataFrameTransformStateAndStatsTests extends AbstractSerializingDataFrameTestCase<DataFrameTransformStateAndStats> {

    public static DataFrameTransformStateAndStats randomDataFrameTransformStateAndStats() {
        return new DataFrameTransformStateAndStats(randomAlphaOfLengthBetween(1, 10),
                DataFrameTransformStateTests.randomDataFrameTransformState(),
                DataFrameIndexerTransformStatsTests.randomStats());
    }

    @Override
    protected DataFrameTransformStateAndStats doParseInstance(XContentParser parser) throws IOException {
        return DataFrameTransformStateAndStats.PARSER.apply(parser, null);
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

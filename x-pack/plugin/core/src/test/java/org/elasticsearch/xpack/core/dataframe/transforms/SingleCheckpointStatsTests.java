/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.dataframe.transforms;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;

public class SingleCheckpointStatsTests extends AbstractSerializingDataFrameTestCase<SingleCheckpointStats>
{
    public static SingleCheckpointStats randomSingleCheckpointStats() {
        return new SingleCheckpointStats(randomNonNegativeLong(), randomNonNegativeLong());
    }

    @Override
    protected SingleCheckpointStats doParseInstance(XContentParser parser) throws IOException {
        return SingleCheckpointStats.fromXContent(parser);
    }

    @Override
    protected SingleCheckpointStats createTestInstance() {
        return randomSingleCheckpointStats();
    }

    @Override
    protected Reader<SingleCheckpointStats> instanceReader() {
        return SingleCheckpointStats::new;
    }

}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.dataframe.transforms;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xpack.core.dataframe.DataFrameField;

import java.io.IOException;
import java.util.Collections;

public class DataFrameIndexerTransformStatsTests extends AbstractSerializingTestCase<DataFrameIndexerTransformStats> {

    protected static ToXContent.Params TO_XCONTENT_PARAMS = new ToXContent.MapParams(
        Collections.singletonMap(DataFrameField.FOR_INTERNAL_STORAGE, "true"));

    @Override
    protected DataFrameIndexerTransformStats createTestInstance() {
        return randomStats();
    }

    @Override
    protected Writeable.Reader<DataFrameIndexerTransformStats> instanceReader() {
        return DataFrameIndexerTransformStats::new;
    }

    @Override
    protected DataFrameIndexerTransformStats doParseInstance(XContentParser parser) {
        return DataFrameIndexerTransformStats.fromXContent(parser);
    }

    public static DataFrameIndexerTransformStats randomStats() {
        return randomStats(randomAlphaOfLength(10));
    }

    public static DataFrameIndexerTransformStats randomStats(String transformId) {
        return new DataFrameIndexerTransformStats(transformId, randomLongBetween(10L, 10000L),
            randomLongBetween(0L, 10000L), randomLongBetween(0L, 10000L), randomLongBetween(0L, 10000L), randomLongBetween(0L, 10000L),
            randomLongBetween(0L, 10000L), randomLongBetween(0L, 10000L), randomLongBetween(0L, 10000L), randomLongBetween(0L, 10000L),
            randomLongBetween(0L, 10000L));
    }

    @Override
    protected ToXContent.Params getToXContentParams() {
        return TO_XCONTENT_PARAMS;
    }

    public void testMerge() throws IOException {
        String transformId = randomAlphaOfLength(10);
        DataFrameIndexerTransformStats emptyStats = new DataFrameIndexerTransformStats(transformId);
        DataFrameIndexerTransformStats randomStats = randomStats(transformId);

        assertEquals(randomStats, emptyStats.merge(randomStats));
        assertEquals(randomStats, randomStats.merge(emptyStats));

        DataFrameIndexerTransformStats randomStatsClone = copyInstance(randomStats);

        DataFrameIndexerTransformStats trippleRandomStats = new DataFrameIndexerTransformStats(transformId, 3 * randomStats.getNumPages(),
                3 * randomStats.getNumDocuments(), 3 * randomStats.getOutputDocuments(), 3 * randomStats.getNumInvocations(),
                3 * randomStats.getIndexTime(), 3 * randomStats.getSearchTime(), 3 * randomStats.getIndexTotal(),
                3 * randomStats.getSearchTotal(), 3 * randomStats.getIndexFailures(), 3 * randomStats.getSearchFailures());

        assertEquals(trippleRandomStats, randomStats.merge(randomStatsClone).merge(randomStatsClone));
    }
}

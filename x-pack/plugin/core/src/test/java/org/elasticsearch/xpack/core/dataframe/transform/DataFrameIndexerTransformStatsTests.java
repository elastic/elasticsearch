/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.dataframe.transform;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;

import java.io.IOException;

public class DataFrameIndexerTransformStatsTests extends AbstractSerializingTestCase<DataFrameIndexerTransformStats> {
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
        return new DataFrameIndexerTransformStats(randomLongBetween(0L, 10000L), randomLongBetween(0L, 10000L),
                randomLongBetween(0L, 10000L), randomLongBetween(0L, 10000L), randomLongBetween(0L, 10000L), randomLongBetween(0L, 10000L),
                randomLongBetween(0L, 10000L), randomLongBetween(0L, 10000L), randomLongBetween(0L, 10000L), randomLongBetween(0L, 10000L));
    }

    public void testMerge() throws IOException {
        DataFrameIndexerTransformStats emptyStats = new DataFrameIndexerTransformStats();
        DataFrameIndexerTransformStats randomStats = randomStats();

        assertEquals(randomStats, emptyStats.merge(randomStats));
        assertEquals(randomStats, randomStats.merge(emptyStats));

        DataFrameIndexerTransformStats randomStatsClone = copyInstance(randomStats);

        DataFrameIndexerTransformStats trippleRandomStats = new DataFrameIndexerTransformStats(3 * randomStats.getNumPages(),
                3 * randomStats.getNumDocuments(), 3 * randomStats.getOutputDocuments(), 3 * randomStats.getNumInvocations(),
                3 * randomStats.getIndexTime(), 3 * randomStats.getSearchTime(), 3 * randomStats.getIndexTotal(),
                3 * randomStats.getSearchTotal(), 3 * randomStats.getIndexFailures(), 3 * randomStats.getSearchFailures());

        assertEquals(trippleRandomStats, randomStats.merge(randomStatsClone).merge(randomStatsClone));
    }
}
